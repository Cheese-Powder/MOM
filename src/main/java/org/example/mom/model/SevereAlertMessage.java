package org.example.mom.model;

public record SevereAlertMessage(
        String device_id,
        String timestamp,
        double error_ratio_in_window,
        int window_seconds_s,
        String message
) {
}
