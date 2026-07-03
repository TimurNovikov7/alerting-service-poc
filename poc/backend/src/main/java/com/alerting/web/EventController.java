package com.alerting.web;

import com.alerting.config.EventSourceProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    private final JdbcTemplate clickHouseJdbcTemplate;
    private final ObjectMapper objectMapper;
    private final EventSourceProperties eventSourceProperties;

    public EventController(
            @Qualifier("clickHouseJdbcTemplate") JdbcTemplate clickHouseJdbcTemplate,
            ObjectMapper objectMapper,
            EventSourceProperties eventSourceProperties) {
        this.clickHouseJdbcTemplate = clickHouseJdbcTemplate;
        this.objectMapper = objectMapper;
        this.eventSourceProperties = eventSourceProperties;
    }

    @GetMapping
    public List<Map<String, Object>> list(
            @RequestParam(required = false) String sourceId,
            @RequestParam(defaultValue = "50") int limit) {

        List<String> tables = sourceId != null
                ? List.of("events_" + sourceId.replaceAll("[^a-zA-Z0-9_]", "_"))
                : eventSourceProperties.getSources().stream()
                        .map(s -> "events_" + s.getId().replaceAll("[^a-zA-Z0-9_]", "_"))
                        .collect(Collectors.toList());

        List<Map<String, Object>> result = new ArrayList<>();

        for (String table : tables) {
            try {
                String sql = String.format(
                        "SELECT * EXCEPT (row_id, ingested_at) " +
                        "FROM %s ORDER BY occurred_at DESC LIMIT %d", table, limit);
                List<Map<String, Object>> rows = clickHouseJdbcTemplate.queryForList(sql);
                for (Map<String, Object> row : rows) {
                    Map<String, Object> event = new LinkedHashMap<>(row);
                    // Parse payload JSON string into a map for easier frontend consumption
                    try {
                        Object payloadRaw = row.get("payload");
                        if (payloadRaw instanceof String payloadStr && !payloadStr.isBlank()) {
                            event.put("payload", objectMapper.readValue(
                                    payloadStr, new TypeReference<Map<String, Object>>() {}));
                        }
                    } catch (Exception ignored) {
                        // Leave payload as-is if it cannot be parsed
                    }
                    result.add(event);
                }
            } catch (Exception e) {
                log.warn("Failed to query ClickHouse table {}: {}", table, e.getMessage());
            }
        }

        // Sort all results by occurred_at descending
        result.sort((a, b) -> {
            Object at = a.get("occurred_at");
            Object bt = b.get("occurred_at");
            if (at instanceof LocalDateTime ldt1 && bt instanceof LocalDateTime ldt2) {
                return ldt2.compareTo(ldt1);
            }
            // Fall back to string comparison if timestamps come back as strings
            String as = at != null ? at.toString() : "";
            String bs = bt != null ? bt.toString() : "";
            return bs.compareTo(as);
        });

        int effectiveLimit = Math.min(limit, result.size());
        return result.subList(0, effectiveLimit);
    }
}
