package org.example.mom.dashboard;

import org.example.mom.model.AnalysisReport;
import org.example.mom.model.LogMessage;
import org.example.mom.model.SevereAlertMessage;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class DashboardState {

    /** Max raw log lines kept per device in memory (and pushed over WebSocket). */
    private static final int MAX_RECENT_LOG_LINES = 120;

    private final Map<String, DevicePanel> devices = new ConcurrentHashMap<>();
    private final AtomicLong severeAlertTotal = new AtomicLong();

    public void recordLog(LogMessage log) {
        if (log.device_id() == null || log.device_id().isBlank()) {
            return;
        }
        long sec = System.currentTimeMillis() / 1000;
        DevicePanel p = devices.computeIfAbsent(log.device_id(), DevicePanel::new);
        p.addLogBucket(sec, log.log_level());
        p.appendRecentLog(log);
    }

    public void recordAnalysis(AnalysisReport report) {
        if (report.device_id() == null) {
            return;
        }
        DevicePanel p = devices.computeIfAbsent(report.device_id(), DevicePanel::new);
        p.updateFromAnalysis(report);
    }

    public void recordSevereAlert(SevereAlertMessage alert) {
        severeAlertTotal.incrementAndGet();
        if (alert.device_id() == null) {
            return;
        }
        DevicePanel p = devices.computeIfAbsent(alert.device_id(), DevicePanel::new);
        p.updateFromAlert(alert);
    }

    public DashboardSnapshot snapshot() {
        List<DevicePanelView> list = new ArrayList<>();
        for (DevicePanel p : devices.values()) {
            list.add(p.toView());
        }
        list.sort(Comparator.comparing(DevicePanelView::deviceId));
        return new DashboardSnapshot(severeAlertTotal.get(), list);
    }

    public record DashboardSnapshot(long severeAlertsTotal, List<DevicePanelView> devices) {
    }

    public record DevicePanelView(
            String deviceId,
            double warnPercent,
            double errorPercent,
            String lastErrorMessage,
            String lastErrorTimestamp,
            boolean severeWarningActive,
            long severeWarningsForDevice,
            String lastSevereMessage,
            List<SeriesPoint> warnErrorSeries,
            List<LogLineView> recentLogs
    ) {
    }

    /** One line for the dashboard log panel (oldest → newest). */
    public record LogLineView(String timestamp, String logLevel, String message) {
    }

    public record SeriesPoint(long epochSecond, int warnCount, int errorCount) {
    }

    static final class DevicePanel {
        private final String deviceId;
        private volatile double warnPercent;
        private volatile double errorPercent;
        private volatile String lastErrorMessage;
        private volatile String lastErrorTimestamp;
        private volatile boolean severeWarningActive;
        private final AtomicLong severeWarningsForDevice = new AtomicLong();
        private volatile String lastSevereMessage;
        /** second -> [warn, error] */
        private final NavigableMap<Long, int[]> buckets = new ConcurrentSkipListMap<>();
        private final Deque<LogLineView> recentLines = new ArrayDeque<>(MAX_RECENT_LOG_LINES + 8);

        DevicePanel(String deviceId) {
            this.deviceId = deviceId;
        }

        void appendRecentLog(LogMessage log) {
            String ts = log.timestamp() == null ? "" : log.timestamp();
            String lvl = log.log_level() == null ? "" : log.log_level();
            String msg = log.message() == null ? "" : log.message();
            LogLineView line = new LogLineView(ts, lvl, msg);
            synchronized (recentLines) {
                recentLines.addLast(line);
                while (recentLines.size() > MAX_RECENT_LOG_LINES) {
                    recentLines.removeFirst();
                }
            }
        }

        void addLogBucket(long sec, String level) {
            int[] c = buckets.computeIfAbsent(sec, s -> new int[2]);
            if ("WARN".equalsIgnoreCase(level)) {
                c[0]++;
            } else if ("ERROR".equalsIgnoreCase(level)) {
                c[1]++;
            }
            trimBuckets(sec);
        }

        void updateFromAnalysis(AnalysisReport r) {
            this.warnPercent = r.warn_percent();
            this.errorPercent = r.error_percent();
            this.lastErrorMessage = r.last_error_message();
            this.lastErrorTimestamp = r.last_error_timestamp();
            this.severeWarningActive = r.severe_warning_active();
        }

        void updateFromAlert(SevereAlertMessage a) {
            severeWarningsForDevice.incrementAndGet();
            lastSevereMessage = a.message();
        }

        private void trimBuckets(long nowSec) {
            long min = nowSec - 120;
            buckets.headMap(min, false).clear();
        }

        DevicePanelView toView() {
            long nowSec = System.currentTimeMillis() / 1000;
            trimBuckets(nowSec);
            List<SeriesPoint> series = new ArrayList<>();
            for (Map.Entry<Long, int[]> e : buckets.entrySet()) {
                int[] v = e.getValue();
                series.add(new SeriesPoint(e.getKey(), v[0], v[1]));
            }
            series.sort(Comparator.comparingLong(SeriesPoint::epochSecond));
            List<LogLineView> logsCopy;
            synchronized (recentLines) {
                logsCopy = new ArrayList<>(recentLines);
            }
            return new DevicePanelView(
                    deviceId,
                    warnPercent,
                    errorPercent,
                    lastErrorMessage,
                    lastErrorTimestamp,
                    severeWarningActive,
                    severeWarningsForDevice.get(),
                    lastSevereMessage,
                    series,
                    logsCopy
            );
        }
    }
}
