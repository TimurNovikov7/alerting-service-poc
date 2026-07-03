package com.alerting.web;

import com.alerting.rules.model.Rule;
import com.alerting.rules.repository.RuleRepository;
import com.alerting.web.dto.RuleDto;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/rules")
@RequiredArgsConstructor
public class RuleController {

    private final RuleRepository ruleRepository;

    @GetMapping
    public List<RuleDto> list() {
        return ruleRepository.findAll().stream()
                .map(this::toDto)
                .collect(Collectors.toList());
    }

    @GetMapping("/{id}")
    public ResponseEntity<RuleDto> getById(@PathVariable UUID id) {
        return ruleRepository.findById(id)
                .map(rule -> ResponseEntity.ok(toDto(rule)))
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping
    public RuleDto create(@RequestBody Map<String, Object> body) {
        Rule rule = new Rule();
        rule.setName((String) body.get("name"));
        rule.setDescription((String) body.get("description"));
        rule.setSourceId((String) body.get("sourceId"));
        rule.setTriggerEventType((String) body.get("triggerEventType"));
        rule.setCelExpression((String) body.get("celExpression"));
        rule.setCelSummary((String) body.get("celSummary"));
        rule.setResolutionMode((String) body.getOrDefault("resolutionMode", "MANUAL"));
        rule.setSeverity((String) body.getOrDefault("severity", "MEDIUM"));
        rule.setEnabled(true);
        return toDto(ruleRepository.save(rule));
    }

    @PatchMapping("/{id}")
    public ResponseEntity<RuleDto> update(@PathVariable UUID id, @RequestBody Map<String, Object> body) {
        return ruleRepository.findById(id).map(rule -> {
            if (body.containsKey("enabled")) rule.setEnabled((Boolean) body.get("enabled"));
            if (body.containsKey("name")) rule.setName((String) body.get("name"));
            if (body.containsKey("description")) rule.setDescription((String) body.get("description"));
            if (body.containsKey("celExpression")) rule.setCelExpression((String) body.get("celExpression"));
            if (body.containsKey("celSummary")) rule.setCelSummary((String) body.get("celSummary"));
            if (body.containsKey("severity")) rule.setSeverity((String) body.get("severity"));
            if (body.containsKey("resolutionMode")) rule.setResolutionMode((String) body.get("resolutionMode"));
            if (body.containsKey("triggerEventType")) rule.setTriggerEventType((String) body.get("triggerEventType"));
            rule.setUpdatedAt(LocalDateTime.now());
            return ResponseEntity.ok(toDto(ruleRepository.save(rule)));
        }).orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable UUID id) {
        if (!ruleRepository.existsById(id)) {
            return ResponseEntity.notFound().build();
        }
        ruleRepository.deleteById(id);
        return ResponseEntity.noContent().build();
    }

    private RuleDto toDto(Rule rule) {
        RuleDto dto = new RuleDto();
        dto.setId(rule.getId());
        dto.setName(rule.getName());
        dto.setDescription(rule.getDescription());
        dto.setSourceId(rule.getSourceId());
        dto.setTriggerEventType(rule.getTriggerEventType());
        dto.setCelExpression(rule.getCelExpression());
        dto.setCelSummary(rule.getCelSummary());
        dto.setEnabled(rule.isEnabled());
        dto.setResolutionMode(rule.getResolutionMode());
        dto.setSeverity(rule.getSeverity());
        dto.setCreatedAt(rule.getCreatedAt());
        dto.setUpdatedAt(rule.getUpdatedAt());
        return dto;
    }
}
