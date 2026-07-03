package com.alerting.alerts;

import com.alerting.alerts.model.Alert;
import com.alerting.alerts.model.AlertStatus;
import com.alerting.alerts.repository.AlertRepository;
import com.alerting.rules.engine.CelEvaluator;
import com.alerting.rules.repository.RuleRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class AutoResolutionJob {

    private final AlertRepository alertRepository;
    private final RuleRepository ruleRepository;
    private final CelEvaluator celEvaluator;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedDelay = 300_000) // every 5 minutes
    @Transactional
    public void resolveStaleAlerts() {
        List<Alert> candidates = new ArrayList<>();
        candidates.addAll(alertRepository.findByStatusOrderByFiredAtDesc(AlertStatus.OPEN));
        candidates.addAll(alertRepository.findByStatusOrderByFiredAtDesc(AlertStatus.ACKNOWLEDGED));

        log.debug("Auto-resolution job: checking {} open/acknowledged alerts", candidates.size());

        for (Alert alert : candidates) {
            ruleRepository.findById(alert.getRuleId()).ifPresent(rule -> {
                if (!"AUTO".equalsIgnoreCase(rule.getResolutionMode())) {
                    return;
                }
                try {
                    // Restore dimensions map from the stored JSON key
                    Map<String, String> dimensions = objectMapper.readValue(
                            alert.getEntityDimensionValue(), new TypeReference<>() {});
                    // Re-evaluate the rule with an empty payload (only aggregation context matters)
                    boolean stillMatches = celEvaluator.evaluate(
                            rule.getCelExpression(),
                            Map.of(),
                            dimensions
                    );
                    if (!stillMatches) {
                        alert.setStatus(AlertStatus.RESOLVED);
                        alert.setResolvedAt(LocalDateTime.now());
                        alertRepository.save(alert);
                        log.info("Auto-resolved alert {} for rule '{}'", alert.getId(), rule.getName());
                    }
                } catch (Exception e) {
                    log.warn("Auto-resolution check failed for alert {}: {}", alert.getId(), e.getMessage());
                }
            });
        }
    }
}
