package com.alerting.ingestion;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@Builder
public class EventEnvelope {
    private String sourceId;
    /** Named dimension values extracted from the event, e.g. {"punter_id":"123","bet_source":"mobile"}. */
    private Map<String, String> dimensions;
    private String eventType;
    private LocalDateTime occurredAt;
    private Map<String, Object> rawPayload;
    private String rawPayloadJson;
}
