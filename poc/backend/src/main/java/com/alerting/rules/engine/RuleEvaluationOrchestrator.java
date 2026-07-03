package com.alerting.rules.engine;

import com.alerting.alerts.AlertManager;
import com.alerting.ingestion.EventEnvelope;
import com.alerting.rules.repository.RuleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class RuleEvaluationOrchestrator {

    private final RuleRepository ruleRepository;
    private final CelEvaluator celEvaluator;
    private final AlertManager alertManager;

    public void evaluate(EventEnvelope envelope) {
        ruleRepository.findBySourceIdAndEnabled(envelope.getSourceId(), true)
                .stream()
                .filter(rule -> rule.getTriggerEventType() == null ||
                        rule.getTriggerEventType().equalsIgnoreCase(envelope.getEventType()))
                .forEach(rule -> {
                    try {
                        boolean matched = celEvaluator.evaluate(
                                rule.getCelExpression(),
                                envelope.getRawPayload(),
                                envelope.getDimensions()
                        );
                        if (matched) {
                            log.debug("Rule matched: rule={} dimensions={}", rule.getName(), envelope.getDimensions());
                            Map<String, Object> aggSnapshot = celEvaluator.buildAggregationSnapshot(
                                    rule.getCelExpression(), envelope.getDimensions());
                            alertManager.fireAlert(rule, envelope, aggSnapshot);
                        }
                    } catch (Exception e) {
                        log.error("Rule evaluation failed for rule {}: {}", rule.getId(), e.getMessage());
                    }
                });
    }
}
