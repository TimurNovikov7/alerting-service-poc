package com.alerting.web;

import com.alerting.alerts.AlertManager;
import com.alerting.alerts.model.Alert;
import com.alerting.alerts.model.AlertStatus;
import com.alerting.alerts.repository.AlertRepository;
import com.alerting.rules.repository.RuleRepository;
import com.alerting.web.dto.AlertDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/v1/alerts")
@RequiredArgsConstructor
public class AlertController {

    private final AlertRepository alertRepository;
    private final AlertManager alertManager;
    private final RuleRepository ruleRepository;
    private final ObjectMapper objectMapper;

    @GetMapping
    public List<AlertDto> list(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) UUID ruleId) {
        AlertStatus statusEnum = null;
        if (status != null && !status.isBlank()) {
            try {
                statusEnum = AlertStatus.valueOf(status.toUpperCase());
            } catch (IllegalArgumentException e) {
                log.warn("Unknown alert status filter: {}", status);
            }
        }
        List<Alert> alerts;
        if (statusEnum != null && ruleId != null) {
            alerts = alertRepository.findByStatusAndRuleIdOrderByFiredAtDesc(statusEnum, ruleId);
        } else if (statusEnum != null) {
            alerts = alertRepository.findByStatusOrderByFiredAtDesc(statusEnum);
        } else if (ruleId != null) {
            alerts = alertRepository.findByRuleIdOrderByFiredAtDesc(ruleId);
        } else {
            alerts = alertRepository.findAllByOrderByFiredAtDesc();
        }
        return alerts.stream()
                .map(this::toDto)
                .collect(Collectors.toList());
    }

    @GetMapping("/{id}")
    public ResponseEntity<AlertDto> getById(@PathVariable UUID id) {
        return alertRepository.findById(id)
                .map(alert -> ResponseEntity.ok(toDto(alert)))
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/acknowledge")
    public ResponseEntity<Void> acknowledge(@PathVariable UUID id) {
        if (!alertRepository.existsById(id)) {
            return ResponseEntity.notFound().build();
        }
        alertManager.acknowledge(id);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/{id}/resolve")
    public ResponseEntity<Void> resolve(@PathVariable UUID id) {
        if (!alertRepository.existsById(id)) {
            return ResponseEntity.notFound().build();
        }
        alertManager.resolve(id);
        return ResponseEntity.ok().build();
    }

    private AlertDto toDto(Alert alert) {
        AlertDto dto = new AlertDto();
        dto.setId(alert.getId());
        dto.setRuleId(alert.getRuleId());
        dto.setEntityDimensionValue(alert.getEntityDimensionValue());
        dto.setStatus(alert.getStatus().name());
        dto.setSeverity(alert.getSeverity());
        dto.setFiredAt(alert.getFiredAt());
        dto.setAcknowledgedAt(alert.getAcknowledgedAt());
        dto.setResolvedAt(alert.getResolvedAt());

        // Enrich with rule name
        ruleRepository.findById(alert.getRuleId())
                .ifPresent(r -> dto.setRuleName(r.getName()));

        // Parse JSONB snapshots stored as strings
        try {
            if (alert.getMatchedEventSnapshot() != null && !alert.getMatchedEventSnapshot().isBlank()) {
                dto.setMatchedEventSnapshot(
                        objectMapper.readValue(alert.getMatchedEventSnapshot(), Object.class));
            }
            if (alert.getAggregationSnapshot() != null && !alert.getAggregationSnapshot().isBlank()) {
                dto.setAggregationSnapshot(
                        objectMapper.readValue(alert.getAggregationSnapshot(), Object.class));
            }
        } catch (Exception e) {
            log.warn("Failed to parse alert snapshots for alert {}: {}", alert.getId(), e.getMessage());
        }

        return dto;
    }
}
