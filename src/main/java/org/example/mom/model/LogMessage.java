package org.example.mom.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record LogMessage(
        String device_id,
        String timestamp,
        String log_level,
        String message
) {
}
