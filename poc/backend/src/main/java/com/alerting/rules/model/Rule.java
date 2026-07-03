package com.alerting.rules.model;

import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Entity
@Table(name = "rules")
public class Rule {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(nullable = false)
    private String name;

    private String description;

    @Column(name = "source_id", nullable = false)
    private String sourceId;

    @Column(name = "trigger_event_type")
    private String triggerEventType;

    @Column(name = "cel_expression", nullable = false, columnDefinition = "TEXT")
    private String celExpression;

    @Column(name = "cel_summary", columnDefinition = "TEXT")
    private String celSummary;

    @Column(nullable = false)
    private boolean enabled = true;

    @Column(name = "resolution_mode", nullable = false)
    private String resolutionMode = "MANUAL";

    @Column(nullable = false)
    private String severity = "MEDIUM";

    @CreationTimestamp
    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
}
