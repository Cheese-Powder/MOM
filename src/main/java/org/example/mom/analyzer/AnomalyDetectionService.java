package org.example.mom.analyzer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.example.mom.MomJson;
import org.example.mom.RabbitTopology;
import org.example.mom.model.AnalysisReport;
import org.example.mom.model.LogMessage;
import org.example.mom.model.SevereAlertMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Subscribes to raw logs, maintains per-device windows, publishes periodic analysis and severe alerts.
 * <p>
 * System properties: {@code mom.window.n} (default 200), {@code mom.report.period.seconds.t} (default 5),
 * {@code mom.severe.window.seconds.s} (default 30)
 */
public final class AnomalyDetectionService {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        int n = Integer.getInteger("mom.window.n", 200);
        int tSec = Integer.getInteger("mom.report.period.seconds.t", 5);
        int sSec = Integer.getInteger("mom.severe.window.seconds.s", 30);

        String host = envOr("RABBITMQ_HOST", "localhost");
        int port = Integer.parseInt(envOr("RABBITMQ_PORT", "5672"));

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);

        Map<String, DeviceState> devices = new ConcurrentHashMap<>();

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(RabbitTopology.LOG_FANOUT, "fanout", true);
            channel.exchangeDeclare(RabbitTopology.ANALYSIS_FANOUT, "fanout", true);
            channel.exchangeDeclare(RabbitTopology.ALERT_FANOUT, "fanout", true);

            channel.queueDeclare(RabbitTopology.QUEUE_ANALYZER, true, false, false, null);
            channel.queueBind(RabbitTopology.QUEUE_ANALYZER, RabbitTopology.LOG_FANOUT, "");

            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread th = new Thread(r, "analyzer-report");
                th.setDaemon(true);
                return th;
            });

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    LocalDateTime now = LocalDateTime.now();
                    for (Map.Entry<String, DeviceState> e : devices.entrySet()) {
                        DeviceState st = e.getValue();
                        synchronized (st) {
                            if (st.isEmpty()) {
                                continue;
                            }
                            AnalysisReport rep = st.buildReport(now, n, sSec, e.getKey());
                            byte[] body = MomJson.mapper().writeValueAsBytes(rep);
                            channel.basicPublish(RabbitTopology.ANALYSIS_FANOUT, "", null, body);

                            if (rep.severe_warning_active()) {
                                double ratio = st.errorRatioInWindowSeconds(now, sSec);
                                SevereAlertMessage alert = new SevereAlertMessage(
                                        e.getKey(),
                                        now.format(TS),
                                        ratio,
                                        sSec,
                                        "严重告警：最近 " + sSec + " 秒内 ERROR 日志占比 " + String.format("%.1f%%", ratio * 100)
                                                + "，已超过 50% 阈值");
                                byte[] alertBody = MomJson.mapper().writeValueAsBytes(alert);
                                channel.basicPublish(RabbitTopology.ALERT_FANOUT, "", null, alertBody);
                            }
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }, tSec, tSec, TimeUnit.SECONDS);

            channel.basicConsume(RabbitTopology.QUEUE_ANALYZER, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                    try {
                        LogMessage log = MomJson.mapper().readValue(body, LogMessage.class);
                        if (log.device_id() == null || log.device_id().isBlank()) {
                            return;
                        }
                        LocalDateTime ts = parseTimestamp(log.timestamp());
                        DeviceState st = devices.computeIfAbsent(log.device_id(), k -> new DeviceState());
                        synchronized (st) {
                            st.accept(log, ts, n);
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });

            System.out.println("AnomalyDetectionService started. N=" + n + ", T=" + tSec + "s, S=" + sSec + "s. Awaiting logs...");
            Thread.currentThread().join();
        }
    }

    private static LocalDateTime parseTimestamp(String raw) {
        if (raw == null || raw.isBlank()) {
            return LocalDateTime.now();
        }
        try {
            return LocalDateTime.parse(raw.trim(), TS);
        } catch (Exception e) {
            return LocalDateTime.now();
        }
    }

    private static String envOr(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v.trim();
    }

    static final class DeviceState {
        private final Deque<LogMessage> lastN = new ArrayDeque<>();
        private final List<TimedLevel> windowLevels = new ArrayList<>();
        private LogMessage lastError;

        void accept(LogMessage log, LocalDateTime ts, int maxN) {
            lastN.addLast(log);
            trimLastN(maxN);
            windowLevels.add(new TimedLevel(ts, log.log_level()));
            if ("ERROR".equalsIgnoreCase(log.log_level())) {
                lastError = log;
            }
        }

        boolean isEmpty() {
            return lastN.isEmpty();
        }

        AnalysisReport buildReport(LocalDateTime now, int n, int sSec, String deviceId) {
            trimLastN(n);
            pruneWindow(now, sSec);

            int total = lastN.size();
            long err = 0;
            long warn = 0;
            for (LogMessage m : lastN) {
                if ("ERROR".equalsIgnoreCase(m.log_level())) {
                    err++;
                } else if ("WARN".equalsIgnoreCase(m.log_level())) {
                    warn++;
                }
            }
            double errPct = total == 0 ? 0 : (100.0 * err / total);
            double warnPct = total == 0 ? 0 : (100.0 * warn / total);

            String lastErrMsg = lastError != null ? lastError.message() : null;
            String lastErrTs = lastError != null ? lastError.timestamp() : null;

            double ratioS = errorRatioInWindowSeconds(now, sSec);
            boolean severe = ratioS > 0.5 && countWindow(now, sSec) > 0;

            return new AnalysisReport(
                    now.format(TS),
                    deviceId,
                    n,
                    round2(errPct),
                    round2(warnPct),
                    lastErrMsg,
                    lastErrTs,
                    severe
            );
        }

        double errorRatioInWindowSeconds(LocalDateTime now, int sSec) {
            pruneWindow(now, sSec);
            int total = 0;
            int err = 0;
            for (TimedLevel tl : windowLevels) {
                total++;
                if ("ERROR".equalsIgnoreCase(tl.level())) {
                    err++;
                }
            }
            return total == 0 ? 0 : (double) err / total;
        }

        int countWindow(LocalDateTime now, int sSec) {
            pruneWindow(now, sSec);
            return windowLevels.size();
        }

        private void trimLastN(int n) {
            while (lastN.size() > n) {
                lastN.removeFirst();
            }
        }

        private void pruneWindow(LocalDateTime now, int sSec) {
            Iterator<TimedLevel> it = windowLevels.iterator();
            LocalDateTime cutoff = now.minus(sSec, ChronoUnit.SECONDS);
            while (it.hasNext()) {
                TimedLevel tl = it.next();
                if (tl.time().isBefore(cutoff)) {
                    it.remove();
                } else {
                    break;
                }
            }
        }

        private static double round2(double v) {
            return Math.round(v * 100.0) / 100.0;
        }
    }

    private record TimedLevel(LocalDateTime time, String level) {
    }
}
