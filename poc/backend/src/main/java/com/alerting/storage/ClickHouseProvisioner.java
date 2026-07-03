package com.alerting.storage;

import com.alerting.config.EventSourceProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class ClickHouseProvisioner implements ApplicationRunner {

    private final EventSourceProperties eventSourceProperties;
    private final JdbcTemplate clickHouseJdbcTemplate;

    @Value("${clickhouse.kafka-broker}")
    private String kafkaBroker;

    public ClickHouseProvisioner(
            EventSourceProperties eventSourceProperties,
            @Qualifier("clickHouseJdbcTemplate") JdbcTemplate clickHouseJdbcTemplate) {
        this.eventSourceProperties = eventSourceProperties;
        this.clickHouseJdbcTemplate = clickHouseJdbcTemplate;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("Provisioning ClickHouse DDL for {} event sources", eventSourceProperties.getSources().size());
        for (EventSourceProperties.EventSource source : eventSourceProperties.getSources()) {
            try {
                provisionSource(source);
            } catch (Exception e) {
                log.warn("Failed to provision ClickHouse for source {}: {}", source.getId(), e.getMessage());
            }
        }
    }

    private void provisionSource(EventSourceProperties.EventSource source) {
        String sourceId = safeName(source.getId());
        String topic = source.getKafka().getTopic();
        if (topic == null || topic.isBlank()) {
            log.warn("Skipping ClickHouse provisioning for source {} — no topic configured", source.getId());
            return;
        }
        List<EventSourceProperties.EntityConfig.DimensionConfig> dims = source.getEntity().getDimensions();
        if (dims.isEmpty()) {
            log.warn("Skipping ClickHouse provisioning for source {} — no dimensions configured", source.getId());
            return;
        }
        log.info("Provisioning ClickHouse tables for source: {}", sourceId);

        // 1. Kafka engine table — reads raw JSON message as a single String
        String kafkaTable = String.format(
                "CREATE TABLE IF NOT EXISTS kafka_%s (raw String) " +
                "ENGINE = Kafka " +
                "SETTINGS " +
                "  kafka_broker_list = '%s'," +
                "  kafka_topic_list = '%s'," +
                "  kafka_group_name = 'clickhouse-%s'," +
                "  kafka_format = 'JSONAsString'," +
                "  kafka_num_consumers = 1," +
                "  kafka_skip_broken_messages = 10",
                sourceId, kafkaBroker, topic, sourceId);
        executeSafely("kafka_" + sourceId, kafkaTable);

        // 2. Events table — one typed dim_<name> column per configured dimension
        String dimColumnsDdl = dims.stream()
                .map(d -> "  dim_" + safeName(d.getName()) + " String")
                .collect(Collectors.joining(",\n"));
        String orderByDims = dims.stream()
                .map(d -> "dim_" + safeName(d.getName()))
                .collect(Collectors.joining(", "));

        String eventsTable = String.format(
                "CREATE TABLE IF NOT EXISTS events_%s (\n" +
                "  source_id   String,\n" +
                "%s,\n" +
                "  event_type  String,\n" +
                "  occurred_at DateTime,\n" +
                "  payload     String,\n" +
                "  ingested_at DateTime DEFAULT now(),\n" +
                "  row_id      UUID DEFAULT generateUUIDv4()\n" +
                ") ENGINE = MergeTree()\n" +
                "  PARTITION BY toYYYYMM(occurred_at)\n" +
                "  ORDER BY (%s, occurred_at, row_id)\n" +
                "  TTL occurred_at + INTERVAL 90 DAY",
                sourceId, dimColumnsDdl, orderByDims);
        executeSafely("events_" + sourceId, eventsTable);

        // 3. MV: Kafka → events with per-dimension extraction
        String dimSelectSql = dims.stream()
                .map(d -> String.format("  %s AS dim_%s",
                        buildEntityDimSql(d.getField()), safeName(d.getName())))
                .collect(Collectors.joining(",\n"));
        String timestampSql = buildTimestampSql(source.getTimestamp());
        String eventTypeSql  = buildEventTypeSql(source.getEventType());

        String mvKafkaToEvents = String.format(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_to_events_%s\n" +
                "TO events_%s\n" +
                "AS SELECT\n" +
                "  '%s' AS source_id,\n" +
                "%s,\n" +
                "  %s AS event_type,\n" +
                "  %s AS occurred_at,\n" +
                "  raw AS payload\n" +
                "FROM kafka_%s",
                sourceId, sourceId, source.getId(), dimSelectSql, eventTypeSql, timestampSql, sourceId);
        executeSafely("mv_kafka_to_events_" + sourceId, mvKafkaToEvents);

        // 4. Daily rollup MV — SummingMergeTree grouped by all dims + event_type + day
        String numericFieldPath = source.getPayloadFields().stream()
                .filter(f -> "DOUBLE".equalsIgnoreCase(f.getType()) || "FLOAT".equalsIgnoreCase(f.getType()))
                .findFirst()
                .map(f -> f.getPayloadPath() != null ? f.getPayloadPath() : f.getName())
                .orElse("amount");
        String dimColumnsSelect = dims.stream()
                .map(d -> "dim_" + safeName(d.getName()))
                .collect(Collectors.joining(", "));

        String mvDaily = String.format(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_%s\n" +
                "ENGINE = SummingMergeTree\n" +
                "PARTITION BY toYYYYMM(day)\n" +
                "ORDER BY (%s, event_type, day)\n" +
                "TTL day + INTERVAL 30 DAY\n" +
                "AS SELECT\n" +
                "  %s, event_type,\n" +
                "  toDate(occurred_at) AS day,\n" +
                "  count() AS event_count,\n" +
                "  sum(%s) AS total_amount\n" +
                "FROM events_%s\n" +
                "GROUP BY %s, event_type, day",
                sourceId, dimColumnsSelect, dimColumnsSelect,
                jsonExtractFloat("payload", numericFieldPath),
                sourceId, dimColumnsSelect);
        executeSafely("mv_daily_" + sourceId, mvDaily);

        // 5. Monthly rollup MV
        String mvMonthly = String.format(
                "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_%s\n" +
                "ENGINE = SummingMergeTree\n" +
                "PARTITION BY toYYYYMM(month)\n" +
                "ORDER BY (%s, event_type, month)\n" +
                "AS SELECT\n" +
                "  %s, event_type,\n" +
                "  toStartOfMonth(occurred_at) AS month,\n" +
                "  count() AS event_count,\n" +
                "  sum(%s) AS total_amount\n" +
                "FROM events_%s\n" +
                "GROUP BY %s, event_type, month",
                sourceId, dimColumnsSelect, dimColumnsSelect,
                jsonExtractFloat("payload", numericFieldPath),
                sourceId, dimColumnsSelect);
        executeSafely("mv_monthly_" + sourceId, mvMonthly);

        log.info("ClickHouse provisioning complete for source: {}", sourceId);
    }

    private String buildEntityDimSql(String dotPath) {
        if (dotPath == null || dotPath.isBlank()) return "''";
        String[] parts = dotPath.split("\\.", 2);
        if (parts.length == 1) return String.format("JSONExtractString(raw, '%s')", parts[0]);
        return String.format("JSONExtractString(raw, '%s', '%s')", parts[0], parts[1]);
    }

    private String buildTimestampSql(EventSourceProperties.TimestampConfig ts) {
        String field = ts.getField();
        return switch (ts.getFormat().toUpperCase()) {
            case "EPOCH_MILLIS"  -> String.format(
                    "toDateTime(fromUnixTimestamp64Milli(toInt64OrZero(JSONExtractRaw(raw, '%s'))))", field);
            case "EPOCH_SECONDS" -> String.format(
                    "fromUnixTimestamp(toUInt32OrZero(JSONExtractRaw(raw, '%s')))", field);
            default -> String.format("parseDateTimeBestEffort(JSONExtractString(raw, '%s'))", field);
        };
    }

    private String buildEventTypeSql(EventSourceProperties.EventTypeConfig et) {
        if (et.getConstant() != null && !et.getConstant().isBlank())
            return String.format("'%s'", et.getConstant());
        return String.format("JSONExtractString(raw, '%s')", et.getField());
    }

    /**
     * Builds JSONExtractFloat SQL for a field path with one level of dot-notation.
     * "amount"             → JSONExtractFloat(col, 'amount')
     * "betAmount.amount"   → JSONExtractFloat(JSONExtractRaw(col, 'betAmount'), 'amount')
     */
    private static String jsonExtractFloat(String column, String dotPath) {
        String[] parts = dotPath.split("\\.", 2);
        if (parts.length == 1) {
            return String.format("JSONExtractFloat(%s, '%s')", column, parts[0]);
        }
        return String.format("JSONExtractFloat(JSONExtractRaw(%s, '%s'), '%s')", column, parts[0], parts[1]);
    }

    private static String safeName(String name) {
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    private void executeSafely(String objectName, String ddl) {
        try {
            clickHouseJdbcTemplate.execute(ddl);
            log.debug("Provisioned ClickHouse object: {}", objectName);
        } catch (Exception e) {
            log.error("Could not provision ClickHouse object '{}': {}", objectName, e.getMessage());
        }
    }
}
