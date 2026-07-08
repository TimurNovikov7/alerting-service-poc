package com.alerting.ingestion;

import com.alerting.config.EventSourceProperties;
import com.alerting.ingestion.readiness.CHReadinessGate;
import com.alerting.rules.engine.RuleEvaluationOrchestrator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerManager implements ApplicationListener<ContextRefreshedEvent> {

    private final EventSourceProperties eventSourceProperties;
    private final RuleEvaluationOrchestrator ruleEvaluationOrchestrator;
    private final CHReadinessGate chReadinessGate;
    private final ObjectMapper objectMapper;
    private final org.springframework.kafka.core.ConsumerFactory<String, String> kafkaConsumerFactory;

    private final Map<String, ConcurrentMessageListenerContainer<String, String>> containers = new ConcurrentHashMap<>();
    private static final DateTimeFormatter OCCURRED_AT_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        restartConsumers();
    }

    private synchronized void restartConsumers() {
        containers.values().forEach(c -> {
            try { c.stop(); } catch (Exception e) { log.debug("Error stopping container: {}", e.getMessage()); }
        });
        containers.clear();

        for (EventSourceProperties.EventSource source : eventSourceProperties.getSources()) {
            try {
                startConsumer(source);
            } catch (Exception e) {
                log.error("Failed to start consumer for source {}: {}", source.getId(), e.getMessage());
            }
        }
    }

    private void startConsumer(EventSourceProperties.EventSource source) {
        if (source.getKafka().getTopic() == null || source.getKafka().getTopic().isBlank()) {
            log.warn("Skipping source {} — no topic configured", source.getId());
            return;
        }

        ContainerProperties props = new ContainerProperties(source.getKafka().getTopic());
        props.setMessageListener((MessageListener<String, String>) record -> {
            try {
                Map<String, Object> msg = objectMapper.readValue(record.value(), new TypeReference<>() {});

                // 1. Extract all named dimensions using dot-notation paths
                Map<String, String> dimensions = new LinkedHashMap<>();
                for (EventSourceProperties.EntityConfig.DimensionConfig dim : source.getEntity().getDimensions()) {
                    Object val = resolveFieldPath(msg, dim.getField());
                    dimensions.put(dim.getName(), val != null ? val.toString() : "unknown");
                }

                // 2. Extract timestamp
                Object tsRaw = resolveFieldPath(msg, source.getTimestamp().getField());
                LocalDateTime occurredAt = parseTimestamp(tsRaw, source.getTimestamp().getFormat());

                // 3. Extract event type
                String eventType;
                EventSourceProperties.EventTypeConfig etConfig = source.getEventType();
                if (etConfig.getConstant() != null && !etConfig.getConstant().isBlank()) {
                    eventType = etConfig.getConstant();
                } else {
                    Object etRaw = resolveFieldPath(msg, etConfig.getField());
                    eventType = etRaw != null ? etRaw.toString() : "unknown";
                }

                // 4. Build flat payload map for CEL evaluation
                Map<String, Object> flatPayload = buildFlatPayload(msg, source.getPayloadFields());
                String rawPayloadJson = objectMapper.writeValueAsString(flatPayload);

                EventEnvelope envelope = EventEnvelope.builder()
                        .sourceId(source.getId())
                        .dimensions(dimensions)
                        .eventType(eventType)
                        .occurredAt(occurredAt)
                        .rawPayload(flatPayload)
                        .rawPayloadJson(rawPayloadJson)
                        .build();

                // Block until ClickHouse has committed the triggering event (and any
                // cross-referenced sources) so that aggregation queries see a consistent state.
                chReadinessGate.waitForReadiness(record, source.getId());
                ruleEvaluationOrchestrator.evaluate(envelope);
            } catch (Exception e) {
                log.warn("Failed to process Kafka message from topic {}: {}",
                        source.getKafka().getTopic(), e.getMessage());
            }
        });

        Map<String, Object> consumerProps = new HashMap<>(
                ((DefaultKafkaConsumerFactory<String, String>) kafkaConsumerFactory).getConfigurationProperties());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, source.getKafka().getConsumerGroup() + "-eval");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, source.getKafka().getOffsetReset());

        DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(consumerProps);
        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(factory, props);
        // One listener thread per partition, up to the configured partition count, so a
        // burst on one entity's partition doesn't queue up behind unrelated traffic on
        // other partitions of the same topic.
        container.setConcurrency(source.getKafka().getPartitions());
        container.start();
        containers.put(source.getId(), container);
        log.info("Started Kafka consumer for source: {} -> topic: {} (concurrency={})",
                source.getId(), source.getKafka().getTopic(), source.getKafka().getPartitions());
    }

    @SuppressWarnings("unchecked")
    private Object resolveFieldPath(Map<String, Object> map, String dotPath) {
        if (dotPath == null || dotPath.isBlank() || map == null) return null;
        int dot = dotPath.indexOf('.');
        if (dot == -1) return map.get(dotPath);
        Object nested = map.get(dotPath.substring(0, dot));
        if (nested instanceof Map) {
            return resolveFieldPath((Map<String, Object>) nested, dotPath.substring(dot + 1));
        }
        return null;
    }

    private LocalDateTime parseTimestamp(Object value, String format) {
        if (value == null) return LocalDateTime.now();
        try {
            return switch (format.toUpperCase()) {
                case "EPOCH_MILLIS" -> LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(((Number) value).longValue()), ZoneOffset.UTC);
                case "EPOCH_SECONDS" -> LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(((Number) value).longValue()), ZoneOffset.UTC);
                case "ISO_DATETIME" -> {
                    // Normalise basic offset (+0000) → extended (+00:00) so OffsetDateTime.parse works
                    String s = value.toString().replaceAll("([+-])(\\d{2})(\\d{2})$", "$1$2:$3");
                    try { yield OffsetDateTime.parse(s).toLocalDateTime(); }
                    catch (Exception ignored) { yield LocalDateTime.parse(s); }
                }
                default -> LocalDateTime.parse(value.toString(), OCCURRED_AT_FMT);
            };
        } catch (Exception e) {
            log.warn("Cannot parse timestamp '{}' as {}: {}", value, format, e.getMessage());
            return LocalDateTime.now();
        }
    }

    private Map<String, Object> buildFlatPayload(Map<String, Object> msg,
            List<EventSourceProperties.PayloadField> fields) {
        if (fields.isEmpty()) return msg;
        Map<String, Object> flat = new LinkedHashMap<>();
        for (EventSourceProperties.PayloadField field : fields) {
            String path = field.getPayloadPath() != null ? field.getPayloadPath() : field.getName();
            flat.put(field.getName(), resolveFieldPath(msg, path));
        }
        return flat;
    }
}
