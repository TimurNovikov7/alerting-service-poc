package com.alerting.web.dto;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
public class RuleDto {
    private UUID id;
    private String name;
    private String description;
    private String sourceId;
    private String triggerEventType;
    private String celExpression;
    private String celSummary;
    private boolean enabled;
    private String resolutionMode;
    private String severity;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
