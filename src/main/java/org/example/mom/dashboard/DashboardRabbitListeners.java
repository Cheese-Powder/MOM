package org.example.mom.dashboard;

import org.example.mom.MomJson;
import org.example.mom.RabbitTopology;
import org.example.mom.model.AnalysisReport;
import org.example.mom.model.LogMessage;
import org.example.mom.model.SevereAlertMessage;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Component
public class DashboardRabbitListeners {

    private final DashboardState state;
    private final SimpMessagingTemplate messaging;

    public DashboardRabbitListeners(DashboardState state, SimpMessagingTemplate messaging) {
        this.state = state;
        this.messaging = messaging;
    }

    @RabbitListener(queues = RabbitTopology.QUEUE_DASHBOARD_LOGS)
    public void onLog(byte[] body) throws Exception {
        LogMessage log = MomJson.mapper().readValue(body, LogMessage.class);
        state.recordLog(log);
        push();
    }

    @RabbitListener(queues = RabbitTopology.QUEUE_DASHBOARD_ANALYSIS)
    public void onAnalysis(byte[] body) throws Exception {
        AnalysisReport report = MomJson.mapper().readValue(body, AnalysisReport.class);
        state.recordAnalysis(report);
        push();
    }

    @RabbitListener(queues = RabbitTopology.QUEUE_DASHBOARD_ALERTS)
    public void onAlert(byte[] body) throws Exception {
        SevereAlertMessage alert = MomJson.mapper().readValue(body, SevereAlertMessage.class);
        state.recordSevereAlert(alert);
        push();
    }

    private void push() {
        messaging.convertAndSend("/topic/dashboard", state.snapshot());
    }
}
