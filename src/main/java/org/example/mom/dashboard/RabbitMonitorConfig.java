package org.example.mom.dashboard;

import org.example.mom.RabbitTopology;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMonitorConfig {

    @Bean
    public FanoutExchange logsExchange() {
        return new FanoutExchange(RabbitTopology.LOG_FANOUT, true, false);
    }

    @Bean
    public FanoutExchange analysisExchange() {
        return new FanoutExchange(RabbitTopology.ANALYSIS_FANOUT, true, false);
    }

    @Bean
    public FanoutExchange alertsExchange() {
        return new FanoutExchange(RabbitTopology.ALERT_FANOUT, true, false);
    }

    @Bean
    public Queue dashboardLogsQueue() {
        return new Queue(RabbitTopology.QUEUE_DASHBOARD_LOGS, true, false, false);
    }

    @Bean
    public Queue dashboardAnalysisQueue() {
        return new Queue(RabbitTopology.QUEUE_DASHBOARD_ANALYSIS, true, false, false);
    }

    @Bean
    public Queue dashboardAlertsQueue() {
        return new Queue(RabbitTopology.QUEUE_DASHBOARD_ALERTS, true, false, false);
    }

    @Bean
    public Binding bindLogs(FanoutExchange logsExchange, Queue dashboardLogsQueue) {
        return BindingBuilder.bind(dashboardLogsQueue).to(logsExchange);
    }

    @Bean
    public Binding bindAnalysis(FanoutExchange analysisExchange, Queue dashboardAnalysisQueue) {
        return BindingBuilder.bind(dashboardAnalysisQueue).to(analysisExchange);
    }

    @Bean
    public Binding bindAlerts(FanoutExchange alertsExchange, Queue dashboardAlertsQueue) {
        return BindingBuilder.bind(dashboardAlertsQueue).to(alertsExchange);
    }
}
