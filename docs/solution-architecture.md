# Solution Architecture: EventMonitor

**Version:** 1.5  
**Date:** 2026-07-02  
**Status:** Draft  
**Replaces:** Anti-Fraud Service (see CLAUDE.md)

---

## 1. System Context

EventMonitor is a domain-agnostic event monitoring and alerting engine. It consumes domain events from Kafka, evaluates configurable rules against those events and historical aggregations, and publishes structured alert events to a Kafka topic. Downstream systems own all domain actions triggered by alerts.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          External Systems                               │
│                                                                         │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐  │
│  │ Punter Auth │  │  Payments   │  │  Sportsbook  │  │  (future    │  │
│  │  Service    │  │  Service    │  │   Service    │  │  sources)   │  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬───────┘  └──────┬──────┘  │
└─────────┼────────────────┼────────────────┼─────────────────┼─────────┘
          │  Kafka topics  │                │                 │
          ▼                ▼                ▼                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Kafka (primary bus)                             │
│              + RabbitMQ adapter (legacy sources only)                   │
└──────────┬──────────────────────────────────────────────────────────────┘
           │                           │
           │ (rule evaluation path)    │ (storage path)
           ▼                           ▼
┌──────────────────────────┐  ┌────────────────────────────────────────┐
│      EVENT MONITOR       │  │             ClickHouse                 │
│  (Java / Spring Boot)    │  │  Kafka Table Engine consumes directly  │
│                          │  │  — independent consumer group          │
│  ┌──────────────────────┐│  │                                        │
│  │  Event Ingestion     ││  │  kafka_{sourceId}   (Kafka engine)     │
│  │  Rule Engine         ││  │    ↓ MV                                │
│  │  Alert Manager       ││  │  events_{sourceId}  (raw, 90d TTL)     │
│  │  LLM Authoring       ││  │    ↓ MV                                │
│  └──────────────────────┘│  │  mv_daily_{sourceId}   (30d TTL)       │
│                          │  │  mv_monthly_{sourceId} (permanent)     │
│  ┌──────┐ ┌──────┐ ┌────┐│  │                                        │
│  │ PG   │ │Redis │ │Vault││  │  ← also receives backfill inserts      │
│  └──────┘ └──────┘ └────┘│  │    (bulk load, not via Kafka engine)   │
└──────────────────────────┘  └────────────────────────────────────────┘
                                 │
              ┌──────────────────┼─────────────────┐
              ▼                  ▼                  ▼
       alerts.fired         Prometheus          React SPA
       Kafka topic           /Grafana         (rule authoring
    (downstream consumers    (observability)   + alert dashboard)
     own domain actions)
```

### Downstream Consumers of `alerts.fired`

Downstream services subscribe to the `alerts.fired` topic and execute domain actions. EventMonitor has no knowledge of these systems.

| Consumer (example) | Action |
|---|---|
| Account Service | Suspend player, disable withdrawals |
| Notification Service | Send email, SMS, in-app push |
| KYC Service | Trigger document verification |
| Compliance Service | File regulatory report |

---

## 2. Non-Functional Requirements

### Extensibility — Zero-Code Source Onboarding

This is a first-class architectural requirement. The platform is specifically designed so that integrating a new Kafka topic requires no Java code change, no rebuild, and no redeployment.

| Capability | Target |
|---|---|
| Register a new event source (Kafka topic + field schema + dimensions) | YAML stanza in the config Git repo — no code change, no rebuild, no redeployment |
| Storage provisioning for a new source | All 5 ClickHouse objects (`kafka_*`, `events_*`, `mv_kafka_to_events_*`, `mv_daily_*`, `mv_monthly_*`) created automatically on next pod startup or `POST /actuator/refresh` |
| New source available for use in rules | Within 30 s of config Git push, without restarting any pod |
| Number of concurrently active sources | ≥ 50 sources with no degradation to rule evaluation latency or ClickHouse query performance |
| Rule authoring against a new source | Immediately after provisioning — LLM rule generator reads the source schema from config to generate contextually correct CEL expressions |

The architectural elements that jointly deliver this NFR are: YAML source config in Git → Spring Cloud Config Server → `@RefreshScope` beans → `ClickHouseProvisioner` (idempotent, runs on refresh) → `CelEvaluator` (picks up new source functions without restart).

### Throughput

| Metric | Target |
|---|---|
| Sustained ingest | 300 events/sec (~26 M/day) across all tenants and sources combined |
| Peak burst | 3× sustained (900 events/sec) for up to 60 seconds without consumer lag exceeding 10,000 messages |
| CEL evaluation | > 50,000 evaluations/sec per JVM instance; scales linearly with pod count |

### Latency

| Operation | Target |
|---|---|
| Rule evaluation end-to-end (Kafka message received → alert written to PostgreSQL) | p99 < 2 s at sustained throughput; bounded by ClickHouse KE flush interval (configure `stream_flush_interval_ms ≤ 1000 ms` to achieve this target) |
| ClickHouse aggregation query (raw tier, 90-day window, 10 M events/source, 5 tenants) | p99 < 200 ms |
| Alert acknowledge / resolve REST call | p99 < 100 ms |
| LLM rule generation (first attempt, network-bound) | p95 < 10 s |

### Availability & Resilience

- Backend: 99.9 % uptime (active-active pods, no leader election).
- Kafka consumer lag recovers to < 10,000 messages within 5 minutes of a pod restart.
- ClickHouse ingestion path is **independent** of backend availability — ClickHouse Kafka Engine consumers continue during a full backend outage and replay from committed offsets on recovery.
- A ClickHouse query failure in the aggregation path causes the rule to return non-match (safe default); the failure is metered and does not abort the pipeline.
- When `materialized_views_ignore_errors = 0` (required), a ClickHouse MV failure stalls the KE consumer and stops offset commits. The `CHReadinessGate` circuit breaker fires after `MAX_WAIT_MS` (30 s), increments `ch.readiness.timeout.total`, and proceeds with potentially incomplete CH data. A visible stall with alertable metrics is preferable to silent incorrect aggregates.
- A CEL evaluation error (type mismatch, missing field) is caught, metered as `cel.evaluation.errors`, and treated as non-match.

### Scalability

- Stateless evaluation path: adding backend pods increases throughput linearly (Kafka partition-based load distribution, no shared in-process state).
- ClickHouse storage: 1 shard, 2 replicas is the minimum HA configuration at 300 events/sec. Shard count can be increased without schema changes.
- Redis cluster mode is not required at launch; standalone + replica is sufficient for the short-lived, lossy evaluation caches.

### Data Retention

| Tier | Table | Retention |
|---|---|---|
| Raw events | `events_{sourceId}` | 90 days (TTL) |
| Daily rollup | `mv_daily_{sourceId}` | 30 days (TTL) |
| Monthly rollup | `mv_monthly_{sourceId}` | Permanent |
| Alert records | PostgreSQL `alert` table | Permanent (never deleted by the platform) |
| Audit events | PostgreSQL `audit_event` table | Permanent |

### Durability

- Alert output to `alerts.fired` uses a transactional Kafka producer (at-least-once delivery).
- Alert and audit records are durable on PostgreSQL commit.
- ClickHouse ingest is at-least-once via Kafka Engine; `kafka_skip_broken_messages = 10` handles malformed messages without blocking the consumer.

### Security

- All secrets injected from HashiCorp Vault at startup; never in environment variables or config files.
- Every API call validated against Keycloak JWKS endpoint.
- `tenant_id` filter enforced at the application layer on all data-access paths; GLOBAL_ADMIN is the only role that bypasses it.
- LLM API key is backend-only; never returned to the browser or logged.
- CEL evaluation is fully sandboxed — no I/O, no reflection, no access to JVM internals.

### Observability

All metrics listed in §12 are exposed at `/actuator/prometheus` and scraped by the existing Grafana stack. No additional observability tooling is required by the platform itself.


---

## 3. Component Architecture

### 3.1 Backend: Java 17 / Spring Boot 2.7

The backend is a single deployable JAR. Internally it is organised into vertical slices; each slice owns its own controllers, services, repositories, and domain objects.

```
event-monitor/
  src/main/java/.../
    ├── ingestion/          # Kafka/RabbitMQ consumers, event normalisation
    │     ├── kafka/        # GenericKafkaConsumer (config-driven, one per source)
    │     ├── amqp/         # RabbitMQAdapter (legacy)
    │     ├── schema/       # SchemaRegistryClient
    │     ├── readiness/    # CHReadinessGate (CH offset tracking, cross-source dependency map)
    │     └── model/        # EventEnvelope (internal DTO)
    │
    ├── storage/            # ClickHouse DDL provisioning (no write path — ClickHouse pulls from Kafka)
    │     └── provisioner/  # ClickHouseProvisioner (idempotent DDL for all 5 objects per source)
    │
    ├── rules/              # Rule model, CEL evaluation, aggregation queries
    │     ├── engine/       # RuleEvaluationOrchestrator, CelEvaluator
    │     ├── aggregation/  # AggregationQueryBuilder, AggregationResultCache
    │     ├── governance/   # RuleGovernanceService, template CRUD, publish flow
    │     └── model/        # RuleTemplate, TenantRuleInstance (JPA entities)
    │
    ├── authoring/          # LLM-assisted rule authoring
    │     ├── llm/          # LLMRuleAuthoringService, CelValidator
    │     └── prompt/       # Prompt template + examples library
    │
    ├── alerts/             # Alert lifecycle and Kafka output
    │     ├── manager/      # AlertManager, auto-resolution scheduler
    │     ├── producer/     # AlertOutputProducer (Kafka)
    │     └── model/        # Alert, AuditEvent (JPA entities)
    │
    ├── tenancy/            # Keycloak JWT, TenantContextFilter, role enforcement
    ├── config/             # EventSourceConfig YAML loading from Spring Cloud Config Server (@RefreshScope)
    ├── web/                # REST controllers (one per resource)
    ├── job/                # Scheduled jobs (auto-resolution, cache eviction)
    └── metric/             # Micrometer metric registration
```

### 3.2 Frontend: React SPA

Independently deployed static bundle. No SSR. Communicates with the backend via REST only.

```
event-monitor-ui/
  src/
    ├── screens/
    │     ├── RuleAuthoring/       # NL input → LLM → CEL review → save
    │     ├── TemplateLibrary/     # GLOBAL_ADMIN template catalog
    │     ├── TenantRuleManager/   # TENANT_ADMIN enable/configure
    │     ├── AlertDashboard/      # Alert list, filter, acknowledge, resolve
    │     └── EventSourceBrowser/  # Read-only schema field viewer
    ├── api/                       # Typed REST client (fetch + React Query)
    ├── auth/                      # Keycloak JS adapter, token refresh, guards
    └── components/                # Shared UI components
```

---

## 4. Data Architecture

### 4.1 ClickHouse: Event Storage and Aggregation

ClickHouse is the single source of truth for all domain events. Tables and materialized views are auto-provisioned per registered event source.

#### Retention Tiers

```
Tier          Table                    Retention    Granularity    Use case
────────────────────────────────────────────────────────────────────────────
Raw           events_{sourceId}        90 days      Per-event      Exact queries,
                                                                   recent windows
Daily MV      mv_daily_{sourceId}      30 days      Per day        Medium windows
                                                                   (7d, 14d, 30d)
Monthly MV    mv_monthly_{sourceId}    Permanent    Per month      Long-term,
                                                                   lifetime totals
```

Lifetime totals are computed at query time by summing all rows in `mv_monthly_{sourceId}` for the entity — no additional summary table required.

#### Auto-Provisioned DDL (per source — 5 objects)

`ClickHouseProvisioner` creates all five objects at startup if they do not exist. Idempotent — safe to run on every startup. The exact DDL is generated from the source's YAML config; the schema below uses placeholder notation for config-driven values.

```sql
-- 1. Kafka engine table — cursor into the Kafka topic, not a storage table.
--    ClickHouse manages its own consumer group (clickhouse-{sourceId}),
--    independent of the Java app's consumer group.
CREATE TABLE kafka_{sourceId} (raw String)
ENGINE = Kafka
SETTINGS
  kafka_broker_list          = '{kafka.brokers}',
  kafka_topic_list           = '{kafka.topic}',
  kafka_group_name           = 'clickhouse-{sourceId}',
  kafka_format               = 'JSONAsString',   -- target: Avro with schema_registry_url
  kafka_num_consumers        = 1,
  kafka_skip_broken_messages = 10;

-- 2. Raw events storage table.
--    One typed dim_<name> column per dimension declared in the source YAML config.
--    MergeTree (append-only): dedup belongs at the application layer, not the storage layer.
--    row_id in ORDER BY guarantees every row has a unique sort position.
CREATE TABLE events_{sourceId} (
  tenant_id      String,
  dim_<name1>    String,    -- one column per configured dimension, e.g. dim_punter_id
  dim_<name2>    String,    -- e.g. dim_bet_source  (absent if source has only one dim)
  event_type     String,
  occurred_at    DateTime,
  payload        String,    -- full event JSON
  ingested_at    DateTime DEFAULT now(),
  row_id         UUID DEFAULT generateUUIDv4()
) ENGINE = MergeTree()
  PARTITION BY toYYYYMM(occurred_at)
  ORDER BY (tenant_id, dim_<name1>[, dim_<name2>], occurred_at, row_id)
  TTL occurred_at + INTERVAL 90 DAY;

-- 3. Pipe: Kafka engine → raw events storage.
--    Per-dimension extraction uses JSONExtractString for string/int JSON fields.
CREATE MATERIALIZED VIEW mv_kafka_to_events_{sourceId}
TO events_{sourceId}
AS SELECT
  '{tenantId}'                                              AS tenant_id,
  toDateTime(fromUnixTimestamp64Milli(
    toInt64OrZero(JSONExtractRaw(raw, '{dim1.field}'))))    AS dim_<name1>,   -- EPOCH_MILLIS path
  -- OR for string/ISO fields:
  JSONExtractString(raw, '{dim1.field}')                   AS dim_<name1>,
  JSONExtractString(raw, '{eventType.field}')              AS event_type,
  parseDateTimeBestEffort(JSONExtractString(raw,'{ts.field}')) AS occurred_at, -- ISO_DATETIME path
  raw                                                      AS payload
FROM kafka_{sourceId};

-- 4. Daily rollup MV (auto-pruned at 30 days).
--    GROUP BY includes all dim columns so single-dim and compound queries are both supported.
CREATE MATERIALIZED VIEW mv_daily_{sourceId}
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, dim_<name1>[, dim_<name2>], event_type, day)
TTL day + INTERVAL 30 DAY
AS SELECT
  tenant_id, dim_<name1>[, dim_<name2>], event_type,
  toDate(occurred_at)                           AS day,
  count()                                       AS event_count,
  sum(JSONExtractFloat(payload, '{amountField}')) AS total_amount
FROM events_{sourceId}
GROUP BY tenant_id, dim_<name1>[, dim_<name2>], event_type, day;

-- 5. Monthly rollup MV (permanent — no TTL).
CREATE MATERIALIZED VIEW mv_monthly_{sourceId}
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (tenant_id, dim_<name1>[, dim_<name2>], event_type, month)
AS SELECT
  tenant_id, dim_<name1>[, dim_<name2>], event_type,
  toStartOfMonth(occurred_at)                   AS month,
  count()                                       AS event_count,
  sum(JSONExtractFloat(payload, '{amountField}')) AS total_amount
FROM events_{sourceId}
GROUP BY tenant_id, dim_<name1>[, dim_<name2>], event_type, month;
```

**Notes:**
- The number of `dim_*` columns and their names are read from the source YAML `entity.dimensions` list at provisioning time. A login source with one dimension (`punter_id`) produces `dim_punter_id String`. A bet source with two dimensions (`punter_id`, `bet_source`) produces two columns.
- This design enables both single-dimension queries (`WHERE dim_punter_id = ?`) and compound queries (`WHERE dim_punter_id = ? AND dim_bet_source = ?`) using a single table per source.
- `SummingMergeTree` MVs merge rows with the same ORDER BY key during background operations. Always use `sum(event_count)` at query time, never rely on a single-row point read.
- `kafka_skip_broken_messages = 10` prevents a malformed message from blocking the consumer. Skips are observable via ClickHouse `system.kafka_log`.
- `row_id` is never exposed in the event feed API (`SELECT * EXCEPT (row_id, ingested_at)`).

#### Consumer Groups and Consistency Guarantee

Each Kafka Engine table manages its own independent consumer group:

```
kafka_{sourceId}  →  consumer group: clickhouse-{sourceId}
```

Three provisioned sources produce three separate consumer groups (`clickhouse-punter_login`, `clickhouse-external_bet`, `clickhouse-withdrawal`) with independent offset sequences.

**MV chain is synchronous.** When KE flushes a Kafka batch, all MV inserts complete before the Kafka offset is committed:

```
KE reads Kafka batch
  → INSERT into events_{sourceId}           ← synchronous
      → mv_daily_{sourceId} fires           ← synchronous (attached to events_*)
      → mv_monthly_{sourceId} fires         ← synchronous (attached to events_*)
  → all inserts complete
  → KE commits offset to Kafka consumer group
```

Therefore `committed_offset > event_offset` guarantees that `events_*`, `mv_daily_*`, and `mv_monthly_*` all contain the data for that event. The Java app evaluation path exploits this guarantee via `CHReadinessGate` (§5.4).

**Hard infrastructure requirement:** `materialized_views_ignore_errors = 0` (ClickHouse server default). If set to `1`, MV failures are silently swallowed, offsets are committed against incomplete aggregates, and the consistency guarantee breaks without any observable signal. This setting must be enforced in all ClickHouse deployment configs.

#### Tier Selection Logic

| Requested window | Tier queried |
|---|---|
| ≤ 90 days, exact precision | Raw `events_{sourceId}` |
| ≤ 30 days, day-level precision acceptable | `mv_daily_{sourceId}` |
| > 30 days or month-level precision acceptable | `mv_monthly_{sourceId}` |
| Lifetime | `SELECT sum(total_amount) FROM mv_monthly_{sourceId}` |

The `AggregationQueryBuilder` selects the tier automatically based on the `windowDays` argument in the CEL custom function call.

### 4.2 PostgreSQL: Operational Data

All mutable operational state lives in PostgreSQL.

#### Core Tables

```
rule_template          — global rule definitions (CEL, params schema, status)
tenant_rule_instance   — per-tenant activation with parameter overrides
alert                  — alert records with lifecycle state + snapshots
audit_event            — governance audit trail (all state transitions)
event_source           — registered sources (mirrored from YAML for UI queries)
```

#### Key Indexes

```sql
-- Deduplication: fast lookup of OPEN alerts for same rule+entity
CREATE INDEX idx_alert_dedup
  ON alert (tenant_rule_instance_id, entity_dimension_value, status)
  WHERE status IN ('OPEN', 'ACKNOWLEDGED');

-- Active rule lookup per tenant at evaluation time
CREATE INDEX idx_instance_active
  ON tenant_rule_instance (tenant_id, enabled)
  INCLUDE (template_id, parameter_overrides);

-- Audit trail query by tenant + time range
CREATE INDEX idx_audit_tenant_time
  ON audit_event (tenant_id, occurred_at DESC);
```

### 4.3 Redis: Short-lived Caches

| Cache | Key pattern | TTL | Purpose |
|---|---|---|---|
| Active rule cache | `rules:{tenantId}` | 30s | Avoid per-event PostgreSQL reads for active rules |
| Aggregation result cache | `agg:{tenantId}:{sourceId}:{entityDim}:{windowDays}:{field}` | 60s | Deduplicate ClickHouse queries when multiple rules share an aggregation |
| CEL program cache | In-process `ConcurrentHashMap` | JVM lifetime | Compiled CEL programs keyed on `(templateId, paramHash)` |

The aggregation result cache is lossy by design — a cache miss causes a ClickHouse query, not incorrect data. Cache eviction is acceptable under memory pressure.

**Interaction with offset tracking:** The cache stores CH query results, not events. A cache hit returns a result computed during a previous evaluation and may not include events that arrived since then, undermining the offset tracking guarantee for back-to-back events on the same entity. To preserve exact counts, the cache key for the current entity must be **invalidated before evaluation** (DEL before query, not after). A hit is then only possible when no new events for this entity have arrived since the last evaluation.

---

## 5. Event Flow

### 5.1 Normal Event Processing

Two independent consumer groups operate on the same Kafka topic simultaneously — one in the Java app (rule evaluation), one in ClickHouse (storage). The evaluation path has a one-way readiness dependency on the ClickHouse consumer (§5.4): rule evaluation waits until ClickHouse has committed past the event's offset before querying aggregates. ClickHouse ingestion remains fully independent — it continues uninterrupted during Java app outages.

```
Kafka topic: punter.auth.login
  │
  ├─── Consumer group: event-monitor (Java app)
  │      │
  │    1. GenericKafkaConsumer deserialises via Schema Registry Avro decoder
  │      │
  │    2. EventNormaliser builds EventEnvelope:
  │       { sourceId, tenantId, entityDimensionValue, eventType,
  │         occurredAt, rawPayload, schemaVersion }
  │      │
  │    3. CHReadinessGate.waitForReadiness(record, sourceId)   ← see §5.4
  │       — OFFSET_PAST: wait until clickhouse-{sourceId} committed offset
  │         on (record.topic(), record.partition()) > record.offset()
  │       — END_OFFSET: for each cross-referenced source in active rules,
  │         snapshot end offsets of referenced topic now, wait until
  │         clickhouse-{refSourceId} committed offset ≥ snapshot
  │       — 100 ms poll interval; 30 s MAX_WAIT_MS circuit breaker
  │      │
  │    4. RuleEvaluationOrchestrator.evaluate(envelope)
  │         │
  │         5. Load active TenantRuleInstances from Redis cache
  │            (cache miss → PostgreSQL query → repopulate cache)
  │         │
  │         6. For each matching rule instance:
  │            │
  │            6a. Resolve parameter overrides into CEL expression
  │            │
  │            6b. Build EvaluationContext:
  │                { event fields from rawPayload,
  │                  agg_* functions bound to AggregationQueryLayer }
  │            │
  │            6c. CelEvaluator.evaluate(compiledProgram, context)
  │                │
  │                │  (if CEL encounters agg_count(...) during eval)
  │                └──▶ AggregationQueryBuilder.query(...)
  │                         → check AggregationResultCache (Redis)
  │                         → cache miss: ClickHouse query on correct tier
  │                         → cache result, return value to CEL
  │            │
  │            6d. Result = true?
  │                YES → AlertManager.fireAlert(envelope, ruleInstance, context)
  │                NO  → discard (metered as alerts.not_matched)
  │         │
  │         7. AlertManager checks for existing OPEN alert (dedup index)
  │            OPEN exists → suppress (metered as alerts.suppressed)
  │            NOT EXISTS  → INSERT Alert record (OPEN state)
  │                         → AlertOutputProducer.publish(alert)
  │                           → Kafka: alerts.fired topic
  │
  └─── Consumer group: clickhouse-{sourceId} (ClickHouse Kafka Table Engine)
         │
         kafka_{sourceId} table reads messages
         │
         mv_kafka_to_events_{sourceId} pipes rows into events_{sourceId}
         │
         mv_daily_{sourceId} and mv_monthly_{sourceId} update incrementally
```

**Failure independence:** A Java app restart does not affect ClickHouse ingestion — CH continues consuming from its own committed offset. A ClickHouse consumer lag **does** affect rule evaluation: `CHReadinessGate` blocks evaluation until CH catches up or the circuit breaker fires (30 s). This is intentional — a visible stall is preferable to evaluating against incomplete aggregates.

**Note:** steps 3–7 run once per raw event, independently. When several events for the same entity land within one CH flush window, `CHReadinessGate` can release more than one of them for evaluation at nearly the same moment — see §5.5 for why this can produce more than one alert for what is logically a single triggering incident.

### 5.2 Auto-Resolution Flow (Aggregation-Based Rules)

```
Every 5 minutes — Spring @Scheduled job:

1. SELECT all OPEN/ACKNOWLEDGED alerts WHERE rule.resolutionMode = AUTO
2. For each alert:
   a. Re-evaluate rule's aggregation conditions against current ClickHouse data
      for same (tenantId, entityDimensionValue)
   b. Condition still true? → no-op
      Condition now false?  → UPDATE alert SET status = RESOLVED,
                               resolved_at = now(), resolved_by = NULL
                             → publish RESOLVED state to alerts.fired
```

### 5.3 Rule Authoring Flow (LLM-Assisted)

```
Analyst: POST /api/v1/authoring/generate-cel
  body: { sourceId, entityDimensionField, description: "alert when..." }
  │
1. Fetch schema fields for sourceId from SchemaRegistryClient
2. Build prompt:
   - CEL syntax guide
   - Available custom agg functions + signatures
   - Schema field names + types for sourceId
   - 2-3 curated CEL examples
   - Analyst's natural language description
3. POST to Anthropic API (API key from Vault)
4. Parse response → { celExpression, plainEnglishSummary }
5. CelValidator.validate(celExpression, schemaFields):
   - Compile CEL with type declarations derived from schema
   - If compile error:
     → append error + schema context to prompt → retry (max 3 attempts)
   - If valid: return { celExpression, plainEnglishSummary, valid: true }
6. UI shows CEL + summary side-by-side for analyst review
7. Analyst edits CEL (optional) → re-validates on save
8. POST /api/v1/templates (DRAFT status)
   GLOBAL_ADMIN activates → POST /api/v1/templates/{id}/publish
```

### 5.4 CH Offset Tracking — Aggregation Consistency Guarantee

Before evaluating any rule, the Java app must ensure the triggering event and all events on cross-referenced sources are present in ClickHouse and visible in all materialized views. `CHReadinessGate` implements this guarantee.

#### Why offset tracking, not a fixed delay

A fixed processing delay (e.g. "wait 5 s before querying CH") assumes KE flush latency < delay. Any CH slowdown silently breaks the guarantee — evaluation proceeds against incomplete data with no observable signal. Offset tracking is deterministic: it directly observes whether CH has consumed past the event's Kafka offset, adapts to actual flush speed, and fails loudly when CH is stalled.

#### Two wait modes

| Mode | Applied to | Condition |
|---|---|---|
| `OFFSET_PAST` | Triggering source | `CH committed offset(topic, partition) > event.offset()` |
| `END_OFFSET` | Each cross-referenced source | `CH committed offset(topic, partition) ≥ snapshotted end offset` |

`END_OFFSET` snapshots the topic high watermark once when evaluation begins. New events produced after the snapshot are not waited for — they define the evaluation boundary.

#### Source dependency map

At startup, `CHReadinessGate` scans all active rules to build a static map of which CH consumer groups each triggering source must wait for:

```
punter_login → [
  SourceWaitSpec(group="clickhouse-punter_login", topic="punter-auth-success-login", mode=OFFSET_PAST),
  SourceWaitSpec(group="clickhouse-external_bet",  topic="ebs_bets",                  mode=END_OFFSET)
]
external_bet → [
  SourceWaitSpec(group="clickhouse-external_bet", topic="ebs_bets", mode=OFFSET_PAST)
]
```

Rules referencing only their own source require only `OFFSET_PAST` — no `END_OFFSET` wait and no additional latency.

#### Background offset poller

CH committed offsets and topic end offsets are refreshed every 100 ms by a background thread — not on every event:

```java
@Scheduled(fixedDelay = 100)
void refresh() {
    chCommittedOffsets = adminClient.listConsumerGroupOffsets(allChGroups);
    topicEndOffsets    = adminClient.listOffsets(allSourceTopics, OffsetSpec.latest());
}
```

This limits Kafka Admin API calls to ~10/sec regardless of event throughput.

#### Circuit breaker

If the CH offset does not advance within `MAX_WAIT_MS` (default 30 s), the wait is abandoned, `ch.readiness.timeout.total` is incremented, and evaluation proceeds. `MAX_WAIT_MS` exhausted is a strong signal of CH or KE failure and must fire a page to the on-call team.

#### Consistency guarantee summary

| Guarantee | Status | Condition |
|---|---|---|
| Current event in `events_*` at eval time | Deterministic | `materialized_views_ignore_errors = 0` |
| Current event in `mv_daily_*` at eval time | Deterministic | Same |
| Current event in `mv_monthly_*` at eval time | Deterministic | Same |
| Cross-referenced source events at eval time | Deterministic | `END_OFFSET` mode + above |
| Aggregation result cache freshness | Best-effort | Cache TTL (60 s); lossy by design |

### 5.5 Known Limitation: Batched Ingestion Can Produce Duplicate Alerts

`CHReadinessGate` guarantees an event is *visible* in ClickHouse before evaluation — it says nothing about *which other events in the same batch are also now visible*. Because ClickHouse's Kafka Table Engine flushes on a fixed interval (`kafka_flush_interval_ms`, e.g. 3 s), not per message, several events for the same entity that arrive within one flush window are committed together, as a single batch.

**Mechanism:**
1. Events E1..E4 for the same `(sourceId, entityDimensionValue)` are produced in a tight burst — well within one flush window.
2. ClickHouse's Kafka Engine flushes all four together. `CHReadinessGate`'s `OFFSET_PAST` wait (§5.4) is satisfied for E1, E2, E3, and E4 at essentially the same instant, since the committed offset now exceeds all four events' offsets simultaneously.
3. `RuleEvaluationOrchestrator` evaluates each event independently, querying the aggregate's *current* value at evaluation time — not a value scoped to "the state immediately after this specific event." Once the batch commits, the aggregate already reflects all four events.
4. If the rule's threshold is crossed by the batch as a whole (e.g. `agg_count > 3` becomes true only once all 4 are counted), then E1's evaluation, E2's, E3's, and E4's can each independently observe the already-crossed aggregate and each conclude the condition is true — even though, semantically, only one of them "caused" the crossing.

**Why the existing dedup guard doesn't fully prevent this:** `AlertManager.fireAlert` (§5.1 step 7) suppresses a new alert only while one is currently `OPEN` or `ACKNOWLEDGED` for the same `(ruleId, entityDimensionValue)`. It has no memory of an alert that was fired and already `RESOLVED` moments earlier from the same batch. If E1's evaluation fires and its alert is resolved (by an analyst, an integration, or an auto-resolution job) before E2/E3/E4 finish evaluating, the dedup check no longer sees an open alert to suppress against — E2 (or E3, or E4) can then fire a second, spurious alert for the same underlying incident.

**When this surfaces in practice:** rarely under normal operation, since alerts are typically reviewed and resolved by an analyst over minutes to hours — far slower than the sub-second window between a batch's events evaluating. It is readily reproducible under fast automated resolution (e.g. test/benchmark tooling resolving within ~200 ms of detection) or with a short `kafka_flush_interval_ms` combined with tightly-bursted source events.

**Not yet implemented — candidate mitigations:**
- Debounce/coalesce rule evaluation per `(ruleId, entityDimensionValue)` so a burst is evaluated once, after it settles, rather than once per raw event.
- Extend the dedup check to also suppress firing if an alert for the same `(ruleId, entityDimensionValue)` was `RESOLVED` within a short cooldown window, not only while `OPEN`/`ACKNOWLEDGED`.
- Key the fired alert to the specific set of contributing event offsets (or the aggregate snapshot that triggered it), so re-evaluation of the same batch is recognisably a duplicate rather than a new incident.

---

## 6. Rule Model

### 6.1 Rule Template Structure

```
RuleTemplate
  ├── name, description
  ├── triggerSourceId?         — null = matches any source
  ├── triggerEventType?        — null = matches any event type
  ├── entityDimensionField     — e.g., "$.player_id", "$.ip_address"
  ├── celExpression            — full CEL boolean expression
  ├── celSummary               — plain-English description (LLM-generated)
  ├── parameterSchema          — { name: { type, defaultValue } }
  │     e.g., { "threshold": { "type": "double", "defaultValue": 1000.0 } }
  ├── resolutionMode           — MANUAL | AUTO
  ├── severity                 — INFO | LOW | MEDIUM | HIGH | CRITICAL
  └── status                   — DRAFT | ACTIVE | ARCHIVED
```

### 6.2 CEL Expression Examples

```python
# Simple event-level condition
payload.amount > 10000.0 && payload.currency == "USD"

# Aggregation condition only
agg_count("player_login", "player_id", 1) > 10

# Mixed: event-level + aggregation
payload.amount > 500.0 &&
agg_sum("player_deposit", "player_id", "amount", 24) > 5000.0 &&
agg_distinct_count("player_deposit", "player_id", "payment_bin", 7) >= 3

# Lifetime total
agg_lifetime_sum("player_withdraw", "player_id", "amount") > 100000.0

# With parameterized threshold (resolved before compilation)
payload.amount > ${threshold} &&
agg_count("player_deposit", ${dimSpec}, ${windowDays}) > ${minCount}
```

### 6.3 CEL Custom Function Registry

| Function | Signature | Return | ClickHouse tier |
|---|---|---|---|
| `agg_count` | `(sourceId: string, dimSpec: string, windowDays: int): long` | long | auto-selected |
| `agg_sum` | `(sourceId: string, dimSpec: string, field: string, windowDays: int): double` | double | auto-selected |
| `agg_max` | `(sourceId: string, dimSpec: string, field: string, windowDays: int): double` | double | raw or daily |
| `agg_distinct_count` | `(sourceId: string, dimSpec: string, field: string, windowDays: int): long` | long | raw only |
| `agg_lifetime_sum` | `(sourceId: string, dimSpec: string, field: string): double` | double | monthly rollup sum |

`dimSpec` is a pipe-separated string of dimension names to scope the aggregation. A single name (`"punter_id"`) filters on one dimension column. Multiple names (`"punter_id|bet_source"`) AND-filter on all listed columns — both dimension values are taken from the triggering event's dimensions map. The available dimension names for a source are declared in its YAML config.

Dimension values are injected from the current event's dimensions map (populated at ingestion time from the YAML `entity.dimensions` config) and are not visible in the CEL expression.

### 6.4 Multi-Tenant Rule Hierarchy

```
RuleTemplate (global, authored by GLOBAL_ADMIN)
  │
  ├── TenantRuleInstance: ru-msk  (enabled=true,  threshold=1500)
  ├── TenantRuleInstance: es-ams  (enabled=true,  threshold=1000)
  ├── TenantRuleInstance: dk-ams  (enabled=false, threshold=1000)
  └── TenantRuleInstance: com-knk (enabled=true,  threshold=2000)
```

Parameter resolution at evaluation time:
```
effectiveExpression = template.celExpression
  .replace("${threshold}", instance.parameterOverrides["threshold"]
                           ?? template.parameterSchema["threshold"].defaultValue)
```

---

## 7. Alert Lifecycle

```
                    ┌─────────────────────────────────────────┐
  Rule fires   ───▶ │               OPEN                      │
  (dedup check)     │  firedAt, matchedEventSnapshot,         │
                    │  aggregationSnapshot captured at fire    │
                    └──────┬──────────────────┬───────────────┘
                           │                  │
              Analyst      │                  │  Auto-resolution
              acknowledges │                  │  (aggregation drops
                           ▼                  │   below threshold)
                    ┌──────────────┐          │  OR manual resolve
                    │ ACKNOWLEDGED │          │
                    └──────┬───────┘          │
                           │                  │
                           │  Resolved        │
                           ▼                  ▼
                    ┌──────────────────────────────┐
                    │          RESOLVED            │
                    │  resolvedBy: UUID (manual)   │
                    │  resolvedBy: null (auto)     │
                    └──────────────────────────────┘
                               (terminal)
```

**Deduplication rule:** No new `OPEN` alert is created if an alert with `status IN (OPEN, ACKNOWLEDGED)` already exists for the same `(tenantRuleInstanceId, entityDimensionValue)`.

---

## 8. Event Source Configuration

New event sources are registered via YAML files stored in a dedicated Git repository and served to the application by **Spring Cloud Config Server**. No code change, no application redeploy required.

### Configuration Flow

```
Git repo (event-monitor-config)
  event-sources/
    player-login.yaml
    player-deposit.yaml
    player-bet.yaml
    ...
          │
          │  PR review + merge
          ▼
    Git main branch
          │
          │  CI/CD pipeline: POST /actuator/refresh
          │  (or Spring Cloud Bus broadcast)
          ▼
  Spring Cloud Config Server
    (serves YAML to all event-monitor pods)
          │
          │  @RefreshScope hot reload
          ▼
  Running event-monitor pods
    → new EventSourceConfig beans instantiated
    → ClickHouseProvisioner provisions DDL for new source
    → GenericKafkaConsumer subscribed to new topic
```

Adding a new source: operator opens a PR in `event-monitor-config`, review + merge triggers CI/CD which calls `/actuator/refresh` on the Config Server. Running pods pick up the new source within seconds — no restart.

`dataAvailableSince` is updated by the operator via the REST API (not in the YAML), so it never requires a config repo commit.

### YAML Schema

```yaml
# event-monitor-config Git repo: event-sources/player-login.yaml
id: player_login
displayName: "Player Login Events"
kafka:
  topic: punter.auth.login
  consumerGroup: event-monitor
  offsetReset: earliest              # replay available Kafka history on first registration
schema:
  registrySubject: player-login-value
  version: latest                    # or specific version number
entity:
  dimensionField: "$.player_id"      # JSONPath into payload
  type: PLAYER                       # for UI display only
aggregatableFields:
  - name: amount
    payloadPath: "$.amount"
    type: DOUBLE
  - name: duration_seconds
    payloadPath: "$.session.duration"
    type: DOUBLE
retention:
  rawDays: 90
  dailyRollupDays: 30
  monthlyRollup: PERMANENT
backfill:
  mode: NONE                         # NONE | KAFKA_ONLY | MANUAL_REQUIRED
  dataAvailableSince:                # set by operator once backfill is complete
```

`mode` values:
- `NONE` — new data stream, no history exists. Data availability starts at registration time.
- `KAFKA_ONLY` — history within Kafka's retention window is sufficient. Replay via `offsetReset: earliest` covers it automatically.
- `MANUAL_REQUIRED` — source has months/years of pre-Kafka history. Bulk load must be run by an operator before the source is considered fully operational.

### Startup Sequence for New Source

```
1. Spring Cloud Config Server serves YAML from Git repo
   → EventSourceConfig bean created (@RefreshScope)

2. SchemaRegistryClient.fetchSchema(registrySubject) — validates subject exists

3. ClickHouseProvisioner.provision(sourceConfig):
   Checks information_schema.tables and creates any missing objects (idempotent):

   a. kafka_{sourceId}               — Kafka Table Engine (ClickHouse consumer group)
   b. events_{sourceId}              — raw storage table (90d TTL)
   c. mv_kafka_to_events_{sourceId}  — pipes Kafka engine → storage table
   d. mv_daily_{sourceId}            — daily rollup MV (30d TTL)
   e. mv_monthly_{sourceId}          — monthly rollup MV (permanent)

   Once kafka_{sourceId} is created, ClickHouse begins consuming from the topic
   immediately using its own consumer group (clickhouse-{sourceId}).
   offsetReset for the ClickHouse consumer group: earliest on first creation.

4. GenericKafkaConsumer instance created and subscribed to kafka.topic
   (consumer group: event-monitor)
   - If first registration (no committed offset): offsetReset=earliest replays
     all events still within Kafka's retention window for rule evaluation

5. Source mirrored to PostgreSQL event_source table (backfillStatus derived from YAML)

6. Source visible in /api/v1/event-sources
   - backfill.mode=MANUAL_REQUIRED → shown as ⚠ Backfill Pending in UI
```

---

## 9. Historical Event Backfill

When a new source is registered, its ClickHouse tables start empty. Aggregation-based rules referencing this source will under-fire until sufficient history is loaded. This section defines how history is backfilled and how incomplete data is surfaced to rule authors and alert consumers.

### 9.1 Backfill Scenarios

| Scenario | Mode | Mechanism |
|---|---|---|
| New data stream, no history | `NONE` | No action. `dataAvailableSince = now()`. |
| History within Kafka retention (typically 7–30 days) | `KAFKA_ONLY` | `offsetReset: earliest` — consumer replays automatically on first start. |
| Pre-Kafka history exists (months/years) | `MANUAL_REQUIRED` | Operator runs bulk load. Two paths depending on data availability. |

### 9.2 Backfill Path A: Raw Event Bulk Load

Used when full event history is available (e.g., exportable from source system DB, data lake, or S3 archive).

```
Operator exports historical events as CSV / Parquet / JSON
  │
  ▼
clickhouse-client bulk insert into events_{sourceId}
  (via HTTP API or clickhouse-client --query)
  │
  ▼
ClickHouseProvisioner.repopulateMaterializedViews(sourceId):
  ALTER TABLE mv_daily_{sourceId} MODIFY QUERY ...  -- not supported in ClickHouse
  -- instead: INSERT INTO mv_daily_{sourceId}_target
  --          SELECT ... FROM events_{sourceId}      -- one-time backfill query
  │
  ▼
Operator calls POST /api/v1/event-sources/{id}/backfill/complete
→ sets dataAvailableSince in PostgreSQL
→ backfillStatus transitions to COMPLETE
```

**ClickHouse MV backfill note:** Materialized views only populate from new inserts by default. When backfilling raw events into an already-created MV, run a manual INSERT into the MV's backing table:

```sql
-- Backfill daily rollup backing table from newly loaded raw events
INSERT INTO mv_daily_player_deposit_target
SELECT
  tenant_id, entity_dim, event_type,
  toDate(occurred_at) AS day,
  count()         AS event_count,
  sum(JSONExtractFloat(payload, 'amount')) AS total_amount
FROM events_player_deposit
WHERE occurred_at < now() - INTERVAL 1 DAY   -- historical portion only
GROUP BY tenant_id, entity_dim, event_type, day;

-- Same for monthly rollup
INSERT INTO mv_monthly_player_deposit_target
SELECT tenant_id, entity_dim, event_type,
  toStartOfMonth(occurred_at) AS month,
  count() AS event_count,
  sum(JSONExtractFloat(payload, 'amount')) AS total_amount
FROM events_player_deposit
WHERE occurred_at < toStartOfMonth(now())
GROUP BY tenant_id, entity_dim, event_type, month;
```

### 9.3 Backfill Path B: Pre-Aggregated Monthly Load

Used when raw events are unavailable or too voluminous (years of data), but monthly summaries can be extracted from a source system report or data warehouse.

The monthly rollup backing table accepts direct inserts — raw events are not required:

```sql
INSERT INTO mv_monthly_player_deposit_target
  (tenant_id, entity_dim, event_type, month, event_count, total_amount)
VALUES
  ('ru-msk', 'player_42', 'DEPOSIT', '2024-01-01', 12,  8500.00),
  ('ru-msk', 'player_42', 'DEPOSIT', '2024-02-01',  7,  3200.00),
  ('ru-msk', 'player_42', 'DEPOSIT', '2024-03-01', 15, 11000.00),
  ...
```

This is sufficient for `agg_lifetime_sum` and long-window rules. Rules referencing the raw tier (windows ≤ 90 days) still require Path A for the recent window.

### 9.4 Backfill State Machine

`backfillStatus` is stored in the PostgreSQL `event_source` table and updated by the operator or automatically:

```
NOT_REQUIRED     — mode=NONE, new source, dataAvailableSince=registrationTime
KAFKA_REPLAY     — mode=KAFKA_ONLY, consumer replaying from earliest offset
                   transitions to COMPLETE automatically when consumer reaches
                   the live position (lag = 0)
PENDING_MANUAL   — mode=MANUAL_REQUIRED, bulk load not yet started
BACKFILLING      — bulk load in progress (operator sets manually)
COMPLETE         — history loaded, dataAvailableSince is accurate and frozen
```

### 9.5 Data Availability Watermark

`dataAvailableSince` is stored on `event_source` in PostgreSQL. It drives two behaviours:

**Rule builder warning (React SPA):**
When a rule references an aggregation window that extends before `dataAvailableSince`, the authoring UI shows:

```
⚠ Aggregation window (90 days) extends before available data
  Data available since: 2025-11-15 (~18 days of history).
  This rule may under-fire until 2026-02-13.
```

**Alert data completeness flag:**
The `aggregationSnapshot` payload on every fired alert includes:

```json
{
  "agg_count_90d": 4,
  "dataCompleteness": "PARTIAL",
  "dataAvailableSince": "2025-11-15",
  "windowRequestedDays": 90,
  "windowAvailableDays": 18
}
```

`dataCompleteness` is `FULL` when `dataAvailableSince` predates `now() - windowDays`, `PARTIAL` otherwise. Downstream consumers of `alerts.fired` can use this flag to adjust automated action thresholds or route partial-confidence alerts to a manual review queue.

### 9.6 Backfill API Endpoint

Operators trigger and track backfill state via the REST API (GLOBAL_ADMIN role):

```
GET  /api/v1/event-sources/{id}/backfill          — current backfillStatus + dataAvailableSince
POST /api/v1/event-sources/{id}/backfill/start    — transition PENDING_MANUAL → BACKFILLING
POST /api/v1/event-sources/{id}/backfill/complete — set dataAvailableSince, transition → COMPLETE
```

No bulk data upload through this API — raw event loading is a direct ClickHouse operation (too voluminous for HTTP API). The API manages state only.

---

## 10. Security Architecture  

### Authentication & Authorisation

```
React SPA
  │  Keycloak JS Adapter
  │  → redirects to Keycloak login on first access
  │  → obtains JWT, attaches as Bearer token to every API call
  │  → handles token refresh transparently
  │
  ▼
Backend REST API
  │  TenantContextFilter (OncePerRequestFilter)
  │  → validates JWT signature against Keycloak JWKS endpoint
  │  → extracts claims: sub (userId), tenant_id, roles
  │  → stores in TenantContext (ThreadLocal)
  │
  ├── GLOBAL_ADMIN  → full access, no tenant_id filter applied
  ├── TENANT_ADMIN  → scoped to own tenant_id on all queries
  └── ANALYST       → scoped to own tenant_id; read-only on rules
```

### Role–Capability Matrix

| Capability | GLOBAL_ADMIN | TENANT_ADMIN | ANALYST |
|---|---|---|---|
| Author rule templates | ✓ | | |
| Publish/unpublish templates | ✓ | | |
| Enable/disable rules per tenant | ✓ | ✓ | |
| Override rule parameters | ✓ | ✓ | |
| View alerts (own tenant) | ✓ | ✓ | ✓ |
| Acknowledge / resolve alerts | ✓ | ✓ | ✓ |
| Use LLM rule authoring | ✓ | | ✓ |
| View audit trail (own tenant) | ✓ | ✓ | |
| View audit trail (all tenants) | ✓ | | |
| View event source schemas | ✓ | ✓ | ✓ |

### Secrets Management

All secrets injected from HashiCorp Vault at `/secret/event-monitor`:
- PostgreSQL credentials
- Redis password
- ClickHouse credentials
- Anthropic API key (never returned to browser)
- Kafka SASL credentials (per Kafka cluster)
- Keycloak client secret

---

## 11. Infrastructure Topology

### Deployment

```
                    ┌──────────────────────────────────────┐
                    │         Kubernetes Cluster           │
                    │                                      │
                    │  ┌──────────────────────────────┐   │
                    │  │    event-monitor (×N pods)    │   │
                    │  │   Java 17 / Spring Boot 2.7   │   │
                    │  │   Port 8055 (HTTP)            │   │
                    │  │   Port 11055 (JMX)            │   │
                    │  │   Port 15055 (debug)          │   │
                    │  └──────────────────────────────┘   │
                    │                                      │
                    │  ┌────────────────┐  ┌────────────────────┐  │
                    │  │event-monitor-ui│  │ Spring Cloud       │  │
                    │  │React SPA       │  │ Config Server      │  │
                    │  │Nginx / CDN     │  │ (serves YAML       │  │
                    │  └────────────────┘  │  from Git repo)    │  │
                    │                  └────────────────────┘  │
                    └──────────────────────────────────────────┘
                                    │
              ┌─────────────────────┼──────────────────────┐
              ▼                     ▼                       ▼
     ┌────────────────┐   ┌─────────────────┐   ┌──────────────────┐
     │   ClickHouse   │   │   PostgreSQL    │   │     Redis        │
     │  1 shard,      │   │   primary +     │   │  standalone +    │
     │  2 replicas    │   │   1 replica     │   │  1 replica       │
     └────────────────┘   └─────────────────┘   └──────────────────┘
```

### Horizontal Scaling

The backend is **stateless with respect to request routing** — all state is in PostgreSQL, ClickHouse, or Redis. Any pod can handle any request. Kafka consumer group partitioning distributes event processing load automatically across pods. No leader election required (unlike the existing system's Consul-based Active/Standby).

### Sizing at 300 events/sec

| Component | Minimum | Recommended HA |
|---|---|---|
| Backend pods | 2 | 3 |
| Spring Cloud Config Server | 1 pod | 2 pods (stateless, Git is source of truth) |
| ClickHouse | 1 node, 4 CPU, 16 GB RAM, 500 GB SSD | 2-node replica set |
| PostgreSQL | 2 CPU, 4 GB RAM | Primary + 1 replica |
| Redis | 1 GB RAM | Primary + 1 replica |

ClickHouse storage estimate: ~100 GB/month raw events (columnar compression); monthly rollups are negligible.

---

## 12. Observability

### Prometheus Metrics (Micrometer)

| Metric | Type | Labels | Alert threshold |
|---|---|---|---|
| `kafka.consumer.lag` | Gauge | `topic`, `partition` | > 10,000 |
| `rule.evaluation.latency` | Histogram | `tenantId`, `ruleId` | p99 > 500ms |
| `clickhouse.query.latency` | Histogram | `tier` (raw/daily/monthly) | p99 > 200ms |
| `llm.calls.total` | Counter | `result` (success/retry/failed) | — |
| `llm.calls.retries` | Counter | — | rate > 10% |
| `alerts.fired.total` | Counter | `tenantId`, `ruleId` | drop to 0 for > 1h |
| `alerts.suppressed.total` | Counter | `tenantId`, `ruleId` | — |
| `cel.evaluation.errors` | Counter | `ruleId` | any |
| `event.ingestion.total` | Counter | `sourceId` | — |
| `backfill.lag.events` | Gauge | `sourceId` | — (informational during backfill) |
| `ch.kafka.broken_messages` | Counter | `sourceId` | any (malformed Avro messages skipped by ClickHouse) |
| `ch.kafka.consumer.lag` | Gauge | `sourceId` | > 10,000 (ClickHouse consumer group falling behind) |
| `ch.readiness.wait.duration` | Histogram | `sourceId`, `mode` (offset_past/end_offset) | p99 > 5 s (KE flush significantly delayed) |
| `ch.readiness.timeout.total` | Counter | `sourceId` | any (CH did not catch up within MAX_WAIT_MS — page on-call) |

`ch.kafka.*` metrics are sourced from ClickHouse `system.kafka_consumers`, scraped by a ClickHouse Prometheus exporter — not from Micrometer.

### Health Endpoints

```
GET /actuator/health               — overall UP/DOWN
GET /actuator/health/db            — PostgreSQL connectivity
GET /actuator/health/redis         — Redis connectivity
GET /actuator/health/kafka         — Kafka consumer connectivity
GET /actuator/health/configserver  — Spring Cloud Config Server reachability
GET /actuator/prometheus           — Prometheus scrape endpoint
GET /api/v1/cluster                — pod identity, active source count, rule count
```

---

## 13. Migration Strategy

### Phase 1: Parallel Deployment (Weeks 1–4)

Both the existing anti-fraud service and the new EventMonitor consume the same Kafka topics simultaneously.

```
Kafka events
  ├──▶ Existing anti-fraud service  →  alerts_old PostgreSQL DB
  └──▶ New EventMonitor             →  alerts.fired Kafka topic
                                        + new PostgreSQL DB
```

A daily parity report compares alert output from both systems for the same events. Discrepancies are investigated and resolved before cutover.

### Phase 2: Rule Migration

1. One-time converter reads all `Template` records from the existing PostgreSQL schema
2. For each template: generates a draft `RuleTemplate` using the LLM service (natural language description derived from criterion field names + values)
3. GLOBAL_ADMIN reviews each draft in the new UI, validates the CEL expression, and activates
4. Migrated rules participate in the parallel parity comparison

### Phase 3: Cutover

1. All migrated rules confirmed at parity → switch `alerts.fired` consumers to point at new topic
2. Disable existing anti-fraud service Kafka consumers
3. Existing anti-fraud service remains running in **read-only mode** for 90 days (historical alert queries only)
4. After 90 days: decommission existing service, archive old PostgreSQL DB

---

## 14. PoC vs Target Architecture — Gap Assessment

The PoC proves the core schema design (typed multi-dimension columns, YAML-driven DDL provisioning, configurable field extraction, agg function DSL). The gaps below are all in the runtime path and are addressable with standard Spring Boot patterns — none require new architectural decisions.

| Gap | PoC behaviour | Target behaviour | Fix |
|---|---|---|---|
| **Throughput: 300 events/sec** | ~50 events/sec — rule evaluation blocks the Kafka consumer thread (synchronous PostgreSQL + ClickHouse calls per event) | Async handoff to bounded thread pool; consumer thread returns immediately after deserialization | Hand off `EventEnvelope` to `ThreadPoolExecutor` after parse; evaluation runs off the consumer thread |
| **Redis rule cache (30s TTL)** | Every event hits PostgreSQL for active rules | Rules loaded from Redis cache; cache miss falls back to PostgreSQL and repopulates | `@Cacheable` on `ruleRepository.findBySourceIdAndEnabled`; Spring Cache + Redis |
| **Redis aggregation result cache (60s TTL)** | Every rule evaluation issues a fresh ClickHouse query | Agg results cached per `sourceId:dimValues:aggFunction:windowDays`; cache miss queries ClickHouse; **cache key for current entity is DEL'd before evaluation** to preserve offset tracking exactness guarantee | Wrap `AggregationQueryService` methods with Redis cache; key on all query dimensions; call `DEL agg:{tenantId}:{sourceId}:{entityDim}:*` before invoking `RuleEvaluationOrchestrator.evaluate()` for that entity |
| **ClickHouse aggregation tier routing** | All `agg_count`/`agg_sum`/`agg_max` always query raw `events_X` | `windowDays ≤ 1` → raw; `≤ 30` → `mv_daily_X`; `> 30` → `mv_monthly_X` | Add tier-selection logic in `AggregationQueryService`; all three tables already provisioned |
| **Multi-tenancy (`tenant_id`)** | No `tenant_id` in any table; all data is global | `tenant_id` column on all ClickHouse and PostgreSQL tables; Spring Data `@Filter` for tenant scoping; `TenantContextFilter` extracts claim from JWT | Add `tenant_id String` to ClickHouse DDL; add `tenant_id VARCHAR` Flyway migration; apply Spring Data `@Filter` on all repositories |
| **Kafka alert output (`alerts.fired` topic)** | Alerts stored in PostgreSQL only; no downstream consumers can subscribe | `AlertOutputProducer` publishes to `alerts.fired` after PostgreSQL commit; transactional Kafka producer | Add `spring-kafka` producer bean; call `AlertOutputProducer.publish(alert)` from `AlertManager.fireAlert()` after commit |
| **Prometheus / Micrometer instrumentation** | No metrics exposed | All metrics in §12 exposed at `/actuator/prometheus` | Add `micrometer-registry-prometheus` dependency; instrument 6 key points with `Counter` and `Timer` |
| **`dev.cel:cel-java` type safety at rule-save time** | No compile-time type check; `payload.amount > 500` silently returns `false` if `amount` is a String | CEL type declarations derived from source schema; type errors caught at rule-save time, never at runtime | Part of the `dev.cel:cel-java` migration; declare `payload` as `map(string, dyn)` with typed field bindings |
| **CH offset tracking (`CHReadinessGate`)** | Evaluation proceeds immediately after `EventEnvelope` is built — no wait for CH offset; current event may not be in CH at query time | `CHReadinessGate` component with background Kafka Admin poller (100 ms), `OFFSET_PAST` and `END_OFFSET` wait modes, 30 s circuit breaker | New component; call `CHReadinessGate.waitForReadiness(record, sourceId)` in `KafkaConsumerManager` before passing envelope to `RuleEvaluationOrchestrator` |
| **Cross-source dependency map** | Not implemented — `CHReadinessGate` cannot determine which referenced sources a rule queries without a pre-built map | Scan all active rules at startup per triggering source; build `Map<sourceId, List<SourceWaitSpec>>`; refresh on rule hot-reload | Part of `CHReadinessGate` initialisation; triggered by `@RefreshScope` event |
| **`materialized_views_ignore_errors` enforcement** | Not enforced — default ClickHouse config may allow MV errors to be silently swallowed | Set `materialized_views_ignore_errors = 0` in all ClickHouse deployment manifests; add health check that verifies the setting on startup | Infrastructure/deployment concern; add a `ClickHouseConfigValidator` startup bean that queries `SELECT value FROM system.settings WHERE name = 'materialized_views_ignore_errors'` and fails fast if `!= 0` |

### Items proved by the PoC (no gap)

| Area | Status |
|---|---|
| Typed multi-dimension DDL generation (`dim_<name> String` columns, dynamic `ORDER BY`) | Proved |
| YAML-driven ClickHouse provisioning — 5 objects per source, idempotent | Proved |
| Configurable field extraction (dot-notation, nested JSON, 4 timestamp formats) | Proved |
| `agg_*` function DSL with single-dim and compound `\|`-separated dim specs | Proved |
| Spring Cloud Config Server hot reload via `@RefreshScope` | Proved |
| ClickHouse Kafka Table Engine → MV chain (two independent consumer groups) | Proved |
| LLM-assisted CEL generation (AWS Bedrock; prompt with schema + dim context) | Proved |
| Alert dedup using serialized dimension map as stable key | Proved |
| CEL engine: `dev.cel:cel-java` 0.7.1 with compiled `Program` cache | Proved |
| Typed multi-dimension aggregation scope: single-dim and compound pipe-separated `dimSpec` | Proved |

---

## 15. Key Design Decisions (Rationale Summary)

| Decision | Chosen | Rejected alternatives | Reason |
|---|---|---|---|
| Aggregation backend | ClickHouse + materialized views | Kafka Streams, Esper | On-demand queries support new aggregation expressions without redeploy; no in-memory state loss risk |
| Expression engine | CEL | JEXL, SpEL, custom DSL, SQL WHERE | Sandboxed, typed, no I/O, well-documented grammar — LLMs generate it reliably; purpose-built for policy evaluation |
| Rule authoring UX | Natural language → LLM → CEL | Form builder (AND/OR groups only) | Analysts describe fraud patterns in English; LLM handles translation; CEL provides auditability |
| LLM integration | Backend-only, validation loop | Client-side call | API key security; schema-aware type-checking before returning to UI |
| Multi-tenancy | Logical (single deployment, tenant_id) | Per-region deployments | Eliminates rule duplication across regions; single UI, single audit trail |
| Alert actions | Kafka output only | Built-in action library | Decouples detection from response; downstream services own domain logic; new actions never require alerting platform changes |
| Event source registration | YAML in Git + Spring Cloud Config Server | UI-driven, Consul KV | Git provides PR review, audit trail, and revert for source changes; Config Server serves them with hot reload; no new KV store discipline required |
| Runtime config storage | Spring Cloud Config Server (Git-backed) | Consul KV | Git is the authoritative store — full history, diff, blame, revert; PR review enforces a change-control gate before config reaches running pods |
| Clustering | Stateless, active-active | Consul leader election (existing) | CEL evaluation is stateless; all durable state is in external stores; no failover warm-up problem |
| Historical backfill | YAML-declared backfill mode + direct ClickHouse bulk insert | Auto-backfill via API, streaming replay only | Pre-Kafka history may be years old; bulk insert into ClickHouse is the only practical path at volume; state machine tracks completeness explicitly |
| ClickHouse ingestion | Kafka Table Engine (ClickHouse pulls from Kafka) | Java app pushes via JDBC (ClickHouseWriter) | Removes batching/retry/back-pressure logic from Java app; two independent failure domains; ClickHouse scales its own consumer threads via DDL |
| Aggregation consistency | CH offset tracking — wait until `clickhouse-{sourceId}` committed offset > event offset before querying | Fixed processing delay (wait N seconds before querying) | Fixed delay is probabilistic: any CH slowdown silently produces wrong aggregates with no signal; offset tracking is deterministic, self-adapting to actual KE flush latency, and fails loudly when CH is stalled |
