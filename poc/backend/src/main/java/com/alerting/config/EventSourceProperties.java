package com.alerting.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@RefreshScope
@Configuration
@ConfigurationProperties(prefix = "event-sources")
public class EventSourceProperties {

    private List<EventSource> sources = List.of();

    @Data
    public static class EventSource {
        private String id;
        private String displayName;
        private KafkaConfig kafka = new KafkaConfig();
        private EntityConfig entity = new EntityConfig();
        private TimestampConfig timestamp = new TimestampConfig();
        private EventTypeConfig eventType = new EventTypeConfig();
        private List<PayloadField> payloadFields = List.of();
    }

    @Data
    public static class EntityConfig {
        /**
         * Named dimensions extracted from the event message.
         * The first dimension is the primary one (leftmost in ORDER BY).
         */
        private List<DimensionConfig> dimensions = List.of();

        @Data
        public static class DimensionConfig {
            /** Logical name — becomes column dim_<name> in ClickHouse and the key in agg_*(). */
            private String name;
            /** Dot-notation path in the Kafka JSON message, e.g. "punterId" or "punterInformation.punterId". */
            private String field;
        }
    }

    @Data
    public static class TimestampConfig {
        private String field = "occurred_at";
        /** DATETIME_STRING | ISO_DATETIME | EPOCH_MILLIS | EPOCH_SECONDS */
        private String format = "DATETIME_STRING";
    }

    @Data
    public static class EventTypeConfig {
        private String constant;
        private String field = "event_type";
    }

    @Data
    public static class KafkaConfig {
        private String topic;
        private String consumerGroup = "alerting-platform";
        private String offsetReset = "earliest";
        /**
         * ClickHouse Kafka Engine consumer group name for this source.
         * Defaults to {@code "clickhouse-" + safeName(sourceId)} if not set,
         * matching the group name provisioned by ClickHouseProvisioner.
         */
        private String chConsumerGroup;
    }

    @Data
    public static class PayloadField {
        private String name;
        private String payloadPath;
        private String type = "DOUBLE";
    }
}
