package com.alerting.alerts;

import com.alerting.alerts.model.Alert;
import com.alerting.alerts.model.AlertStatus;
import com.alerting.alerts.repository.AlertRepository;
import com.alerting.ingestion.EventEnvelope;
import com.alerting.rules.model.Rule;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AlertManager {

    private final AlertRepository alertRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public void fireAlert(Rule rule, EventEnvelope envelope, Map<String, Object> aggSnapshot) {
        String dimKey = serializeDimensions(envelope.getDimensions());

        boolean alreadyOpen = alertRepository.findByRuleIdAndEntityDimensionValueAndStatusIn(
                rule.getId(), dimKey, List.of(AlertStatus.OPEN, AlertStatus.ACKNOWLEDGED)
        ).isPresent();

        if (alreadyOpen) {
            log.debug("Suppressing duplicate alert for rule={} dims={}", rule.getId(), dimKey);
            return;
        }

        Alert alert = new Alert();
        alert.setRuleId(rule.getId());
        alert.setEntityDimensionValue(dimKey);
        alert.setStatus(AlertStatus.OPEN);
        alert.setSeverity(rule.getSeverity());

        try {
            Map<String, Object> eventSnapshot = Map.of(
                    "sourceId", envelope.getSourceId(),
                    "eventType", envelope.getEventType(),
                    "occurredAt", envelope.getOccurredAt().toString(),
                    "dimensions", envelope.getDimensions() != null ? envelope.getDimensions() : Map.of(),
                    "payload", envelope.getRawPayload() != null ? envelope.getRawPayload() : Map.of()
            );
            alert.setMatchedEventSnapshot(objectMapper.writeValueAsString(eventSnapshot));
            alert.setAggregationSnapshot(objectMapper.writeValueAsString(aggSnapshot));
        } catch (Exception e) {
            log.warn("Failed to serialize alert snapshots: {}", e.getMessage());
            alert.setMatchedEventSnapshot("{}");
            alert.setAggregationSnapshot("{}");
        }

        alertRepository.save(alert);
        log.info("Alert fired: rule='{}' dims={} severity={}", rule.getName(), dimKey, rule.getSeverity());
    }

    @Transactional
    public void acknowledge(UUID alertId) {
        alertRepository.findById(alertId).ifPresent(alert -> {
            if (alert.getStatus() == AlertStatus.OPEN) {
                alert.setStatus(AlertStatus.ACKNOWLEDGED);
                alert.setAcknowledgedAt(LocalDateTime.now());
                alertRepository.save(alert);
                log.info("Alert acknowledged: {}", alertId);
            }
        });
    }

    @Transactional
    public void resolve(UUID alertId) {
        alertRepository.findById(alertId).ifPresent(alert -> {
            if (alert.getStatus() != AlertStatus.RESOLVED) {
                alert.setStatus(AlertStatus.RESOLVED);
                alert.setResolvedAt(LocalDateTime.now());
                alertRepository.save(alert);
                log.info("Alert resolved: {}", alertId);
            }
        });
    }

    /** Serialize dimensions map with sorted keys for a stable deduplication key. */
    private String serializeDimensions(Map<String, String> dimensions) {
        if (dimensions == null || dimensions.isEmpty()) return "{}";
        try {
            return objectMapper.writeValueAsString(new TreeMap<>(dimensions));
        } catch (Exception e) {
            return dimensions.toString();
        }
    }
}
