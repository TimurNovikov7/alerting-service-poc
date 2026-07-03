package com.alerting.alerts.model;

import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Entity
@Table(name = "alerts")
public class Alert {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "rule_id", nullable = false)
    private UUID ruleId;

    @Column(name = "entity_dimension_value", nullable = false)
    private String entityDimensionValue;

    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private AlertStatus status = AlertStatus.OPEN;

    @Column(nullable = false)
    private String severity;

    @CreationTimestamp
    @Column(name = "fired_at")
    private LocalDateTime firedAt;

    @Column(name = "acknowledged_at")
    private LocalDateTime acknowledgedAt;

    @Column(name = "resolved_at")
    private LocalDateTime resolvedAt;

    @Column(name = "matched_event_snapshot", columnDefinition = "text")
    private String matchedEventSnapshot;

    @Column(name = "aggregation_snapshot", columnDefinition = "text")
    private String aggregationSnapshot;
}
