package com.alerting.rules.repository;

import com.alerting.rules.model.Rule;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface RuleRepository extends JpaRepository<Rule, UUID> {

    List<Rule> findByEnabledTrue();

    List<Rule> findBySourceIdAndEnabled(String sourceId, boolean enabled);
}
