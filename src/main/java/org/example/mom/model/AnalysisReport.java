package org.example.mom.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record AnalysisReport(
        String report_time,
        String device_id,
        int window_size_n,
        double error_percent,
        double warn_percent,
        String last_error_message,
        String last_error_timestamp,
        boolean severe_warning_active
) {
}
