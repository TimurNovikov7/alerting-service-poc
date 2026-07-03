package com.alerting.ingestion.readiness;

import com.alerting.config.EventSourceProperties;
import com.alerting.rules.model.Rule;
import com.alerting.rules.repository.RuleRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Blocks rule evaluation until ClickHouse has durably committed all events that
 * the current evaluation depends on.
 *
 * <p>Background: ClickHouse Kafka Engine flushes in batches (every ~500 ms by default).
 * Without waiting, a rule that fired the moment the triggering event arrived would query
 * CH before that event (and its predecessors) are visible, producing stale aggregates.
 *
 * <p>Two wait modes (§5.4 of solution-architecture.md):
 * <ul>
 *   <li>OFFSET_PAST — triggering source: wait until CH committed offset on the event's
 *       partition is strictly greater than the event's own offset.</li>
 *   <li>END_OFFSET — cross-referenced source: snapshot the topic's high-watermark at
 *       evaluation start, then wait until CH reaches that watermark.</li>
 * </ul>
 *
 * <p>The source dependency map is built at startup by scanning all enabled rules for
 * {@code agg_*(sourceId, ...)} calls that reference a different source than the trigger.
 *
 * <p>Circuit breaker: if CH is still not ready after {@code ch-readiness.max-wait-ms}
 * (default 30 s), evaluation proceeds with a warning — stale aggregates are preferable
 * to an indefinitely blocked consumer thread.
 */
@Slf4j
@Component
public class CHReadinessGate {

    private static final Pattern AGG_SOURCE_PATTERN = Pattern.compile("agg_\\w+\\(\"([^\"]+)\"");

    @Value("${ch-readiness.max-wait-ms:30000}")
    private long maxWaitMs;

    @Value("${ch-readiness.poll-interval-ms:100}")
    private long pollIntervalMs;

    private final AdminClient adminClient;
    private final EventSourceProperties eventSourceProperties;
    private final RuleRepository ruleRepository;

    /** sourceId → wait specs that must be satisfied before evaluating rules for that source */
    private volatile Map<String, List<SourceWaitSpec>> dependencyMap = Map.of();

    /** groupId → (TopicPartition → last committed offset) — refreshed every 100 ms */
    private final ConcurrentHashMap<String, ConcurrentHashMap<TopicPartition, Long>> committedOffsets =
            new ConcurrentHashMap<>();

    /** TopicPartition → high-watermark — refreshed for END_OFFSET topics every 100 ms */
    private final ConcurrentHashMap<TopicPartition, Long> topicEndOffsets = new ConcurrentHashMap<>();

    /** Topics that require end-offset monitoring (cross-referenced sources) */
    private volatile Set<String> endOffsetTopics = Set.of();

    public CHReadinessGate(AdminClient adminClient,
                           EventSourceProperties eventSourceProperties,
                           RuleRepository ruleRepository) {
        this.adminClient = adminClient;
        this.eventSourceProperties = eventSourceProperties;
        this.ruleRepository = ruleRepository;
    }

    // ── startup ────────────────────────────────────────────────────────────────

    /**
     * Scans all enabled rules at startup to build the per-source wait spec list.
     * Re-call this method when rules change (e.g. on RefreshScope event) to keep
     * the dependency map current.
     */
    @PostConstruct
    public void buildDependencyMap() {
        Map<String, EventSourceProperties.EventSource> sourceById = eventSourceProperties.getSources()
                .stream()
                .collect(Collectors.toMap(EventSourceProperties.EventSource::getId, s -> s));

        List<Rule> rules = ruleRepository.findByEnabledTrue();
        Map<String, List<SourceWaitSpec>> map = new HashMap<>();
        Set<String> newEndOffsetTopics = new HashSet<>();

        for (EventSourceProperties.EventSource source : eventSourceProperties.getSources()) {
            List<SourceWaitSpec> specs = new ArrayList<>();

            // OFFSET_PAST: always wait for the triggering source itself
            specs.add(new SourceWaitSpec(chGroupFor(source), source.getKafka().getTopic(), WaitMode.OFFSET_PAST));

            // END_OFFSET: find other sources referenced in CEL expressions
            Set<String> crossSourceIds = new LinkedHashSet<>();
            for (Rule rule : rules) {
                if (!source.getId().equals(rule.getSourceId())) continue;
                Matcher m = AGG_SOURCE_PATTERN.matcher(rule.getCelExpression());
                while (m.find()) {
                    String refId = m.group(1);
                    if (!refId.equals(source.getId())) {
                        crossSourceIds.add(refId);
                    }
                }
            }

            for (String refId : crossSourceIds) {
                EventSourceProperties.EventSource refSource = sourceById.get(refId);
                if (refSource != null) {
                    specs.add(new SourceWaitSpec(
                            chGroupFor(refSource), refSource.getKafka().getTopic(), WaitMode.END_OFFSET));
                    newEndOffsetTopics.add(refSource.getKafka().getTopic());
                } else {
                    log.warn("Rule references unknown source '{}' — skipping END_OFFSET wait spec", refId);
                }
            }

            map.put(source.getId(), Collections.unmodifiableList(specs));
        }

        this.dependencyMap = Collections.unmodifiableMap(map);
        this.endOffsetTopics = Collections.unmodifiableSet(newEndOffsetTopics);
        log.info("CHReadinessGate dependency map built: {}",
                map.entrySet().stream()
                        .map(e -> e.getKey() + "→[" + e.getValue().stream()
                                .map(s -> s.mode() + ":" + s.group())
                                .collect(Collectors.joining(", ")) + "]")
                        .collect(Collectors.joining("; ")));
    }

    // ── background offset refresh ──────────────────────────────────────────────

    @Scheduled(fixedDelay = 100)
    public void refreshOffsets() {
        Set<String> groups = dependencyMap.values().stream()
                .flatMap(Collection::stream)
                .map(SourceWaitSpec::group)
                .collect(Collectors.toSet());

        for (String group : groups) {
            try {
                Map<TopicPartition, OffsetAndMetadata> result = adminClient
                        .listConsumerGroupOffsets(group)
                        .partitionsToOffsetAndMetadata()
                        .get(3, TimeUnit.SECONDS);
                ConcurrentHashMap<TopicPartition, Long> tpMap =
                        committedOffsets.computeIfAbsent(group, g -> new ConcurrentHashMap<>());
                result.forEach((tp, oam) -> tpMap.put(tp, oam.offset()));
            } catch (Exception e) {
                log.debug("Could not refresh CH committed offsets for group {}: {}", group, e.getMessage());
            }
        }

        if (!endOffsetTopics.isEmpty()) {
            refreshEndOffsets();
        }
    }

    private void refreshEndOffsets() {
        // Build the query map from partitions we already know from committed offset entries
        Map<TopicPartition, OffsetSpec> query = new HashMap<>();
        committedOffsets.forEach((group, tpMap) ->
                tpMap.keySet().stream()
                        .filter(tp -> endOffsetTopics.contains(tp.topic()))
                        .forEach(tp -> query.put(tp, OffsetSpec.latest())));
        if (query.isEmpty()) return;

        try {
            adminClient.listOffsets(query)
                    .all()
                    .get(3, TimeUnit.SECONDS)
                    .forEach((tp, info) -> topicEndOffsets.put(tp, info.offset()));
        } catch (Exception e) {
            log.debug("Could not refresh topic end offsets: {}", e.getMessage());
        }
    }

    // ── public API ─────────────────────────────────────────────────────────────

    /**
     * Blocks the calling thread until ClickHouse has consumed all events required
     * for a correct evaluation of rules triggered by {@code record} on source {@code sourceId}.
     *
     * <p>Releases after {@code ch-readiness.max-wait-ms} with a warning if CH is still behind.
     * The circuit breaker logs the timeout but lets evaluation proceed rather than blocking
     * the Kafka consumer thread indefinitely.
     */
    public void waitForReadiness(ConsumerRecord<?, ?> record, String sourceId) {
        List<SourceWaitSpec> specs = dependencyMap.get(sourceId);
        if (specs == null || specs.isEmpty()) return;

        long deadline = System.currentTimeMillis() + maxWaitMs;
        TopicPartition eventTp = new TopicPartition(record.topic(), record.partition());

        for (SourceWaitSpec spec : specs) {
            boolean reached;
            if (spec.mode() == WaitMode.OFFSET_PAST) {
                reached = waitUntilOffsetPast(spec.group(), eventTp, record.offset(), deadline);
            } else {
                // Snapshot end offsets for the referenced topic right now, then wait for CH to catch up
                Map<TopicPartition, Long> snapshot = snapshotEndOffsets(spec.topic());
                reached = waitUntilEndOffsetsReached(spec.group(), snapshot, deadline);
            }
            if (!reached) {
                log.error("ch.readiness.timeout: sourceId={} waitSpec={} — proceeding with potentially stale aggregates",
                        sourceId, spec);
            }
        }
    }

    // ── private wait helpers ───────────────────────────────────────────────────

    /**
     * Kafka committed-offset semantics: committed=N means offsets 0..N-1 have been processed.
     * So "CH has consumed the event at offset E" ↔ "committed offset > E".
     */
    private boolean waitUntilOffsetPast(String group, TopicPartition tp, long targetOffset, long deadline) {
        while (System.currentTimeMillis() < deadline) {
            Long committed = Optional.ofNullable(committedOffsets.get(group))
                    .map(m -> m.get(tp))
                    .orElse(null);
            if (committed != null && committed > targetOffset) return true;
            sleep();
        }
        return false;
    }

    /**
     * Waits until CH's committed offsets for a group reach or exceed the given snapshot.
     * An empty snapshot means the topic had no messages at evaluation start — immediately satisfied.
     */
    private boolean waitUntilEndOffsetsReached(String group, Map<TopicPartition, Long> snapshot, long deadline) {
        if (snapshot.isEmpty()) return true;
        while (System.currentTimeMillis() < deadline) {
            ConcurrentHashMap<TopicPartition, Long> groupOffsets = committedOffsets.get(group);
            if (groupOffsets != null) {
                boolean allReached = snapshot.entrySet().stream().allMatch(e -> {
                    Long committed = groupOffsets.get(e.getKey());
                    return committed != null && committed >= e.getValue();
                });
                if (allReached) return true;
            }
            sleep();
        }
        return false;
    }

    private Map<TopicPartition, Long> snapshotEndOffsets(String topic) {
        return topicEndOffsets.entrySet().stream()
                .filter(e -> e.getKey().topic().equals(topic))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void sleep() {
        try {
            Thread.sleep(pollIntervalMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    /**
     * Derives the ClickHouse Kafka Engine consumer group name for a source.
     * Must match the {@code kafka_group_name} set by ClickHouseProvisioner:
     * {@code 'clickhouse-' + safeName(sourceId)}.
     */
    static String chGroupFor(EventSourceProperties.EventSource source) {
        String configured = source.getKafka().getChConsumerGroup();
        if (configured != null && !configured.isBlank()) return configured;
        return "clickhouse-" + source.getId().replaceAll("[^a-zA-Z0-9_]", "_");
    }
}
