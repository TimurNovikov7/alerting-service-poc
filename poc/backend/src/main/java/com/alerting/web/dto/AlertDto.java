package com.alerting.web.dto;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
public class AlertDto {
    private UUID id;
    private UUID ruleId;
    private String ruleName;
    private String entityDimensionValue;
    private String status;
    private String severity;
    private LocalDateTime firedAt;
    private LocalDateTime acknowledgedAt;
    private LocalDateTime resolvedAt;
    private Object matchedEventSnapshot;
    private Object aggregationSnapshot;
}
