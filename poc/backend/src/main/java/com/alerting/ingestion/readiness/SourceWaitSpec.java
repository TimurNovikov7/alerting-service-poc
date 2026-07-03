package com.alerting.ingestion.readiness;

/**
 * Describes one ClickHouse consumer group that must be waited on before querying
 * aggregates for a given triggering source.
 *
 * @param group  ClickHouse Kafka Engine consumer group name, e.g. "clickhouse-external_bet"
 * @param topic  Kafka topic the group consumes from
 * @param mode   OFFSET_PAST for the triggering source; END_OFFSET for cross-referenced sources
 */
public record SourceWaitSpec(String group, String topic, WaitMode mode) {}
