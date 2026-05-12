package org.example.mom.collector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.example.mom.MomJson;
import org.example.mom.RabbitTopology;
import org.example.mom.model.LogMessage;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Simulates one log collection node (run multiple JVM processes with different node IDs).
 * <p>
 * Usage: {@code LogCollectorNode <unique_node_id>} <br>
 * Env: {@code RABBITMQ_HOST} (default localhost), {@code RABBITMQ_PORT} (default 5672)
 */
public final class LogCollectorNode {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String[] LEVELS = {"INFO", "WARN", "ERROR"};
    /** Steady-state mix; occasional simulated outage bursts ERROR so S-window can exceed 50%. */
    private static final double[] LEVEL_WEIGHT = {0.78, 0.14, 0.08};

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: LogCollectorNode <node_id>");
            System.exit(1);
        }
        String nodeId = args[0];
        String host = envOr("RABBITMQ_HOST", "localhost");
        int port = Integer.parseInt(envOr("RABBITMQ_PORT", "5672"));

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(RabbitTopology.LOG_FANOUT, "fanout", true);

            String deviceId = "device_" + nodeId;
            Random random = new Random(nodeId.hashCode());
            final long[] tick = {0L};
            final boolean[] outage = {false};

            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "log-tick-" + nodeId);
                t.setDaemon(true);
                return t;
            });

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    tick[0]++;
                    // Every ~25s simulate a short outage: mostly ERROR for ~3s (300 ticks ≈ 30s at 100ms)
                    if (tick[0] % 250 == 1) {
                        outage[0] = true;
                    }
                    if (outage[0] && tick[0] % 250 == 35) {
                        outage[0] = false;
                    }
                    String level = outage[0] ? pickLevelOutage(random) : pickLevel(random);
                    String msg = sampleMessage(level, random);
                    String ts = LocalDateTime.now().format(TS);
                    LogMessage log = new LogMessage(deviceId, ts, level, msg);
                    byte[] body = MomJson.mapper().writeValueAsBytes(log);
                    channel.basicPublish(RabbitTopology.LOG_FANOUT, "", null, body);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, 0, 100, TimeUnit.MILLISECONDS);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                scheduler.shutdown();
                try {
                    if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }));

            System.out.println("Collector node " + nodeId + " publishing for " + deviceId + " every 100ms. Ctrl+C to stop.");
            Thread.currentThread().join();
        }
    }

    private static String envOr(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v.trim();
    }

    private static String pickLevel(Random random) {
        double x = random.nextDouble();
        double acc = 0;
        for (int i = 0; i < LEVEL_WEIGHT.length; i++) {
            acc += LEVEL_WEIGHT[i];
            if (x < acc) {
                return LEVELS[i];
            }
        }
        return LEVELS[0];
    }

    /** ~85% ERROR during simulated outage */
    private static String pickLevelOutage(Random random) {
        return random.nextDouble() < 0.85 ? "ERROR" : (random.nextBoolean() ? "WARN" : "INFO");
    }

    private static String sampleMessage(String level, Random random) {
        return switch (level) {
            case "ERROR" -> switch (random.nextInt(3)) {
                case 0 -> "磁盘IO超时";
                case 1 -> "连接数据库失败";
                default -> "服务线程崩溃";
            };
            case "WARN" -> switch (random.nextInt(2)) {
                case 0 -> "GC时间过长";
                default -> "请求队列堆积";
            };
            default -> random.nextBoolean() ? "系统状态正常" : "心跳检测成功";
        };
    }
}
