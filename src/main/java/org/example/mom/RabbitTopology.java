package org.example.mom;

/**
 * Shared RabbitMQ topology for collectors, analyzer, and dashboard.
 */
public final class RabbitTopology {
    public static final String LOG_FANOUT = "mom.logs.fanout";
    public static final String ANALYSIS_FANOUT = "mom.analysis.fanout";
    public static final String ALERT_FANOUT = "mom.alerts.fanout";

    public static final String QUEUE_ANALYZER = "mom.q.analyzer";
    public static final String QUEUE_DASHBOARD_LOGS = "mom.q.dashboard.logs";
    public static final String QUEUE_DASHBOARD_ANALYSIS = "mom.q.dashboard.analysis";
    public static final String QUEUE_DASHBOARD_ALERTS = "mom.q.dashboard.alerts";

    private RabbitTopology() {
    }
}
