package com.alerting.alerts.repository;

import com.alerting.alerts.model.Alert;
import com.alerting.alerts.model.AlertStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface AlertRepository extends JpaRepository<Alert, UUID> {

    Optional<Alert> findByRuleIdAndEntityDimensionValueAndStatusIn(
            UUID ruleId, String entityDimensionValue, List<AlertStatus> statuses);

    List<Alert> findAllByOrderByFiredAtDesc();

    List<Alert> findByStatusOrderByFiredAtDesc(AlertStatus status);

    List<Alert> findByRuleIdOrderByFiredAtDesc(UUID ruleId);

    List<Alert> findByStatusAndRuleIdOrderByFiredAtDesc(AlertStatus status, UUID ruleId);
}
