# PRD: Alerting Platform (Next-Generation Anti-Fraud Detection Engine)

**Date:** 2026-06-02
**Status:** Draft

---

## Problem Statement

The existing anti-fraud service evaluates player events against fraud detection rules that are hardcoded in Java. The criterion type-to-handler mapping lives in a single 51 KB `ProcessorConfig` class. Adding a new rule type, a new event source, or a new aggregation window requires a code change, a full build, and a coordinated redeploy across all five regional deployments (com-knk, ru-msk, by-mns, es-ams, dk-ams).

This creates four compounding problems:

1. **Rule agility is blocked by the release cycle.** Fraud analysts cannot react to emerging patterns without engineering involvement. By the time a new rule ships, the attack window has closed.

2. **Event source onboarding is expensive.** Every new data source (new Kafka topic, new event type from a partner) requires engineering work before any rule can reference it.

3. **Aggregation is brittle.** Pre-computed streaming aggregates are hard to backfill, hard to change, and silently stale when consumers lag. There is no reliable way to query "last 90 days" or "lifetime totals."

4. **Multi-tenancy is operational, not logical.** Five region-specific deployments share no rule library. A rule authored for ru-msk must be manually ported to dk-ams. There is no central template governance, no shared audit trail, and no per-tenant parameter overrides.

The new platform must eliminate all four problems while preserving or improving detection coverage, throughput (~300 events/sec, ~26 M/day), and alert fidelity.

---

## Solution

A config-driven, multi-tenant alerting platform that separates event ingestion, aggregation, rule evaluation, and alert output into independent, independently-deployable modules.

**Core design choices:**

- **Kafka-first event bus** with Confluent Schema Registry. Every event source is described in a YAML config file. New sources are onboarded by dropping a YAML file and reloading config — no code change, no redeploy.
- **ClickHouse as the aggregation backend.** All events are written to ClickHouse. Rules query on demand against raw events (90-day retention), daily rollups (30 days), monthly rollups (permanent), or lifetime totals. Materialized views provide near-pre-computed speed for common windows.
- **CEL (Common Expression Language) as the rule expression engine.** Rules are CEL expressions evaluated against a typed context built from the incoming event payload and ClickHouse aggregation results. CEL is sandboxed, typed, and has no I/O or side effects.
- **LLM-assisted rule authoring.** Analysts describe rules in plain English. A backend service calls the Anthropic API, generates a CEL expression, validates it against the Schema Registry, and returns both the CEL and a plain-English summary for analyst review. Analysts can edit the CEL before saving.
- **Single logical deployment, logical multi-tenancy.** All tenants share one deployment. Every table carries `tenant_id`. Keycloak JWT claims enforce tenant scoping on every query. Three roles: GLOBAL_ADMIN, TENANT_ADMIN, ANALYST.
- **Rule templates with per-tenant publishing.** GLOBAL_ADMINs author parameterized templates. Templates are explicitly published to selected tenants with optional per-tenant parameter overrides. TENANT_ADMINs enable/disable published rules within their jurisdiction.
- **Alert output as a Kafka topic.** The platform fires alerts to `alerts.fired`. Downstream consumers own all domain actions (suspend player, send email, etc.). The alerting platform is a pure detection engine.

---

## User Stories

### Fraud Analyst

**US-01** — As a fraud analyst, I want to describe a new rule in plain English so that I can create it without knowing CEL syntax.

**US-02** — As a fraud analyst, I want to review the CEL expression and plain-English summary generated from my description so that I can verify the rule captures my intent before saving it.

**US-03** — As a fraud analyst, I want to manually edit the generated CEL expression so that I can correct subtle semantic errors the LLM may have introduced.

**US-04** — As a fraud analyst, I want to browse the schema fields available for the event source I am writing a rule against so that I know what fields I can reference in conditions.

**US-05** — As a fraud analyst, I want to set the entity dimension (player_id, IP, BIN, affiliate_id, device_fingerprint, or any schema field) for a rule so that aggregations are grouped correctly for my use case.

**US-06** — As a fraud analyst, I want to reference aggregation windows (e.g., "total deposits in the last 7 days", "number of logins in the last 24 hours", "lifetime withdrawal amount") in rule conditions so that I can detect patterns that span multiple events.

**US-07** — As a fraud analyst, I want to combine event-level conditions and aggregation conditions in a single rule so that I can filter precisely on both the triggering event and historical context.

**US-08** — As a fraud analyst, I want to see the alert dashboard filtered by tenant, rule, and status (OPEN / ACKNOWLEDGED / RESOLVED) so that I can triage the most urgent alerts first.

**US-09** — As a fraud analyst, I want to acknowledge an alert so that colleagues know it is being investigated.

**US-10** — As a fraud analyst, I want to manually resolve an alert so that event-triggered rule alerts are closed when the investigation is complete.

**US-11** — As a fraud analyst, I want to see the matched event snapshot and aggregation snapshot attached to each alert so that I have all the evidence I need without running a separate query.

### Tenant Admin

**US-12** — As a tenant admin, I want to see all rule templates that have been published to my tenant so that I know what detection capability is available to me.

**US-13** — As a tenant admin, I want to enable or disable a published rule template for my tenant without affecting other tenants so that I can control which rules are active in my jurisdiction.

**US-14** — As a tenant admin, I want to override the parameterized thresholds of a published template (e.g., change the deposit threshold from $500 to $200) so that I can tune rules for my jurisdiction's risk appetite without creating a separate template.

**US-15** — As a tenant admin, I want to see the full audit trail of rule activations, deactivations, and parameter changes for my tenant so that I can demonstrate compliance.

**US-16** — As a tenant admin, I want to see alert volume broken down by rule over time so that I can identify rules with abnormally high or low firing rates.

### Global Admin

**US-17** — As a global admin, I want to author a rule template with parameterized thresholds (e.g., a `threshold` variable) so that the same template can be reused across tenants with different tuning.

**US-18** — As a global admin, I want to publish a template to one or more tenants, with optional per-tenant default parameter values, so that I can roll out a new detection capability in a controlled way.

**US-19** — As a global admin, I want to publish a template to all tenants at once so that cross-jurisdictional rules can be deployed without per-tenant repetition.

**US-20** — As a global admin, I want to unpublish a template from a tenant (disabling it and removing it from their view) so that I can retire deprecated rules cleanly.

**US-21** — As a global admin, I want to view the audit trail for any template across all tenants so that I have full governance visibility.

**US-22** — As a global admin, I want to browse the template library and see which tenants each template is currently published to and in what state (enabled/disabled) so that I can manage the global rule catalog.

### Platform Operator / Developer

**US-23** — As a platform operator, I want to register a new event source by creating a YAML config file (declaring the Kafka topic, consumer group, Schema Registry subject, entity dimension field, and retention config) so that no code change or redeploy is required.

**US-24** — As a platform operator, I want ClickHouse tables and materialized views for a new event source to be auto-provisioned on first startup after YAML registration so that I do not have to write DDL manually.

**US-25** — As a platform operator, I want Kafka consumer lag, rule evaluation latency (p50/p95/p99), ClickHouse query latency per retention tier, LLM call count and failure rate, alert firing rate per rule and tenant, and CEL evaluation errors exposed as Prometheus metrics so that I can monitor the system with our existing Grafana stack.

**US-26** — As a platform operator, I want a read-only view of registered event source configurations in the UI so that analysts can see what sources are available without accessing config files directly.

**US-27** — As a platform operator, I want the migration converter to produce a draft rule in the new template model for each existing Java-based template so that the GLOBAL_ADMIN can review and activate them in the new UI without manual rewriting.

---

## Implementation Decisions

### 1. Event Ingestion Layer

**Responsibility:** Consume events from Kafka (and optionally RabbitMQ), validate against Schema Registry, normalize to an internal event envelope, and route to downstream storage and rule evaluation.

**Component structure:**
- `GenericKafkaConsumer` — a single, config-driven consumer class parameterized by source config. One instance is created per registered YAML source on startup. No per-source subclass.
- `EventSourceConfig` (POJO) — deserialized from YAML: `{ sourceId, kafkaTopic, consumerGroup, schemaRegistrySubject, entityDimensionField, retentionConfig }`. Loaded via Spring Cloud Config / Consul; changes trigger hot reload via `@RefreshScope`.
- `SchemaRegistryClient` — wraps the Confluent client. Fetches the Avro/Protobuf schema for a subject; caches per subject+version. Used at startup to validate YAML config and at rule authoring time to provide field type information.
- `EventEnvelope` (internal DTO): `{ sourceId: String, tenantId: String, entityDimensionValue: String, eventType: String, occurredAt: Instant, rawPayload: Map<String, Object>, schemaVersion: int }`. All internal processing operates on this envelope.
- `RabbitMQAdapter` — translates legacy RabbitMQ messages to `EventEnvelope`. Shares the same downstream pipeline. Not on the hot path for new sources.

**Key interaction:** After normalization, `EventEnvelope` is passed concurrently to (a) `ClickHouseWriter` and (b) `RuleEvaluationOrchestrator`. These are fire-and-acknowledge; ClickHouse write failures do not block evaluation but are metered.

---

### 2. Event Storage Layer

**Responsibility:** Persist all events to ClickHouse. Auto-provision tables and materialized views. Manage retention tiers.

**Data model per event source (auto-generated DDL):**

```sql
-- Raw events (ReplacingMergeTree for idempotent writes)
CREATE TABLE events_{sourceId} (
  tenant_id       String,
  entity_dim      String,   -- value of entityDimensionField
  event_type      String,
  occurred_at     DateTime,
  payload         String,   -- JSON blob of full event
  ingested_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
  PARTITION BY toYYYYMM(occurred_at)
  ORDER BY (tenant_id, entity_dim, occurred_at)
  TTL occurred_at + INTERVAL 90 DAY;

-- Daily rollup MV (sums numeric payload fields declared in YAML as aggregatable)
CREATE MATERIALIZED VIEW mv_daily_{sourceId} ...
  POPULATE AS SELECT tenant_id, entity_dim, toDate(occurred_at) AS day,
    count() AS event_count, sum(amount) AS total_amount ...
  ENGINE = SummingMergeTree ...
  TTL day + INTERVAL 30 DAY;

-- Monthly rollup MV (permanent)
CREATE MATERIALIZED VIEW mv_monthly_{sourceId} ...
  ENGINE = SummingMergeTree ... -- no TTL
```

- `ClickHouseProvisioner` runs at startup, checks `information_schema.tables`, and issues CREATE TABLE / CREATE MATERIALIZED VIEW if missing. Idempotent.
- `ClickHouseWriter` batches `EventEnvelope` inserts using ClickHouse JDBC driver. Batch size and flush interval configurable. Writes to `events_{sourceId}`.
- Lifetime totals are not a stored table; they are computed at query time by summing `mv_monthly_{sourceId}` across all months.

---

### 3. Rule Engine

**Responsibility:** At trigger time, evaluate all active rules for the matching tenant and trigger type against the event's context.

**Rule data model (PostgreSQL):**

```
RuleTemplate {
  id: UUID
  name: String
  description: String
  triggerSourceId: String?       -- null = any source
  triggerEventType: String?      -- null = any event type
  entityDimensionField: String   -- configurable per rule
  celExpression: String          -- full evaluated expression
  celSummary: String             -- LLM-generated plain-English
  parameterSchema: JSONB         -- { paramName: { type, defaultValue } }
  createdBy: UUID
  createdAt: Instant
  updatedAt: Instant
  status: DRAFT | ACTIVE | ARCHIVED
}

TenantRuleInstance {
  id: UUID
  templateId: UUID
  tenantId: String
  parameterOverrides: JSONB      -- { paramName: value }
  enabled: Boolean
  publishedAt: Instant
  publishedBy: UUID
  enabledAt: Instant?
  disabledAt: Instant?
}
```

**Evaluation flow:**

1. `RuleEvaluationOrchestrator.evaluate(EventEnvelope)`:
   a. Load all `TenantRuleInstance` where `tenantId = envelope.tenantId` AND `enabled = true` AND trigger matches (`sourceId`, `eventType`). Results come from a short-lived Redis cache (TTL 30s) to avoid per-event PostgreSQL reads.
   b. For each instance, resolve parameter overrides into the CEL expression (simple string substitution of named variables before CEL compilation).
   c. Build `EvaluationContext` (see module 4).
   d. Invoke `CelEvaluator.evaluate(expression, context)`.
   e. If result is `true`, call `AlertManager.fireAlert(...)`.

**`CelEvaluator`:**
- Uses the `dev.cel:cel-java` library.
- Programs are compiled once per (templateId, parameterHash) pair and cached in a `ConcurrentHashMap<String, Program>`. Compilation is the expensive step; evaluation of a compiled program is microseconds.
- Type declarations are derived from the Schema Registry schema at compile time, giving CEL type-checking at rule-save time and fast evaluation at runtime.
- CEL evaluation errors (type mismatch at runtime, missing field) are caught, metered, and treated as non-match (not an exception that aborts the pipeline).

---

### 4. Aggregation Query Layer

**Responsibility:** Given a rule's aggregation condition (e.g., "sum of `amount` for this entity over the last 7 days"), select the appropriate ClickHouse tier and return the result.

**Query selection logic:**

| Window | Tier used |
|---|---|
| <= 90 days, exact precision needed | Raw `events_{sourceId}` |
| <= 30 days, day-level precision acceptable | `mv_daily_{sourceId}` |
| > 30 days, month-level precision acceptable | `mv_monthly_{sourceId}` |
| Lifetime | Sum across all months in `mv_monthly_{sourceId}` |

- `AggregationQueryBuilder` constructs parameterized SQL from the CEL aggregation sub-expression's AST node. Aggregation nodes in CEL are custom functions registered as `agg_sum(sourceId, field, windowDays)`, `agg_count(sourceId, windowDays)`, etc. These are resolved lazily when CEL encounters them during evaluation.
- `AggregationResultCache` (Redis): key = `{tenantId}:{sourceId}:{entityDimValue}:{aggregationKey}:{windowDays}`, TTL = 60 seconds. Prevents redundant ClickHouse queries when multiple rules reference the same aggregation for the same entity within a short window.
- Query results are returned as typed values (`Long`, `Double`) matching the CEL type declaration, preventing runtime type errors.

---

### 5. LLM Rule Authoring Service

**Responsibility:** Translate a natural-language rule description into a valid, schema-typed CEL expression.

**Flow:**

```
Analyst submits: { sourceId, entityDimensionField, naturalLanguageDescription }
  → LLMRuleAuthoringService.generate(request):
      1. Fetch schema fields for sourceId from SchemaRegistryClient
      2. Build prompt: system context (CEL syntax, available custom agg functions,
         schema field names+types) + user description
      3. POST to Anthropic API (backend only; API key from Vault, never in browser)
      4. Parse response: extract { celExpression, plainEnglishSummary }
      5. CelValidator.validate(celExpression, schemaFields):
           - Compile CEL with type declarations
           - If compile error: send error + schema context back to LLM (retry)
           - Max 3 attempts; on 3rd failure return error to UI
      6. Return { celExpression, plainEnglishSummary, valid: true } to UI
```

- The API key is injected from HashiCorp Vault at startup. It is never returned to the browser.
- Each attempt is metered: `llm.calls.total`, `llm.calls.retries`, `llm.calls.failed`.
- The LLM is prompted with the full Avro/Protobuf schema field list (name + type) for the selected source, the custom aggregation function signatures, and 2–3 CEL examples from a curated examples library.
- Analysts can edit the CEL after review. On save, the CEL is re-validated before the template is persisted.

---

### 6. Alert Manager

**Responsibility:** Deduplicate alerts, manage the alert lifecycle state machine, and schedule auto-resolution for aggregation-based rules.

**Alert data model (PostgreSQL):**

```
Alert {
  id: UUID
  tenantId: String
  ruleTemplateId: UUID
  tenantRuleInstanceId: UUID
  entityDimensionValue: String
  status: OPEN | ACKNOWLEDGED | RESOLVED
  severity: INFO | LOW | MEDIUM | HIGH | CRITICAL
  firedAt: Instant
  acknowledgedAt: Instant?
  acknowledgedBy: UUID?
  resolvedAt: Instant?
  resolvedBy: UUID?           -- null for auto-resolution
  matchedEventSnapshot: JSONB -- full EventEnvelope at fire time
  aggregationSnapshot: JSONB  -- ClickHouse query results at fire time
}
```

**Deduplication:** Before creating an alert, `AlertManager` queries PostgreSQL for an existing `Alert` with `status = OPEN AND tenantRuleInstanceId = ? AND entityDimensionValue = ?`. If found, the new trigger is suppressed (metered as `alerts.suppressed`). This query uses a covering index on `(tenant_rule_instance_id, entity_dimension_value, status)`.

**State machine:**

```
OPEN → ACKNOWLEDGED (manual, by analyst or tenant admin)
OPEN → RESOLVED (manual for event-triggered rules; auto for aggregation-based rules)
ACKNOWLEDGED → RESOLVED (manual or auto)
RESOLVED is terminal
```

**Auto-resolution scheduler:** A Spring `@Scheduled` job runs every 5 minutes. For each OPEN or ACKNOWLEDGED alert whose originating rule has `resolutionMode = AUTO`, it re-evaluates the rule's aggregation conditions against current ClickHouse data for the same entity. If the condition is no longer true, the alert transitions to RESOLVED with `resolvedBy = null`.

**Retention:** Alert records are permanent in PostgreSQL (never deleted by the platform).

---

### 7. Rule Governance Service

**Responsibility:** CRUD for rule templates, publish/unpublish to tenants, parameter override resolution, audit trail.

**Publish workflow:**

```
GLOBAL_ADMIN: POST /api/v1/templates/{id}/publish
  body: { tenantIds: [...], parameterDefaults: { tenantId: { param: value } } }
  → Creates TenantRuleInstance per tenantId (enabled=false by default)
  → Writes AuditEvent: TEMPLATE_PUBLISHED

TENANT_ADMIN: POST /api/v1/tenant-rules/{instanceId}/enable
  → Sets TenantRuleInstance.enabled=true
  → Writes AuditEvent: RULE_ENABLED

TENANT_ADMIN: PUT /api/v1/tenant-rules/{instanceId}/parameters
  body: { threshold: 1000, windowDays: 14 }
  → Updates TenantRuleInstance.parameterOverrides
  → Writes AuditEvent: PARAMETERS_UPDATED
```

**Audit trail (PostgreSQL):**

```
AuditEvent {
  id: UUID
  tenantId: String?         -- null for global events
  templateId: UUID?
  instanceId: UUID?
  eventType: String         -- TEMPLATE_CREATED, TEMPLATE_PUBLISHED, RULE_ENABLED, etc.
  actorId: UUID
  actorRole: String
  payload: JSONB            -- before/after state diff
  occurredAt: Instant
}
```

Spring Data Envers is used on `RuleTemplate` and `TenantRuleInstance` for automatic revision tracking in addition to the explicit `AuditEvent` table for queryable governance history.

---

### 8. Multi-tenancy Enforcement

**Responsibility:** Extract `tenant_id` and role from every inbound JWT and enforce tenant scoping on all data access.

- `TenantContextFilter` (Spring `OncePerRequestFilter`): validates the Keycloak JWT, extracts `tenant_id` and `roles` claims, and stores them in a `TenantContext` thread-local.
- All repository methods are annotated or overridden to append `AND tenant_id = :tenantId` using a Spring Data JPA `@Filter` applied globally for `TENANT_ADMIN` and `ANALYST` roles. `GLOBAL_ADMIN` bypasses the filter.
- PostgreSQL Row-Level Security is not used (the application layer is authoritative) but the `tenant_id` column has a NOT NULL constraint and a partial index on all high-read tables.
- Kafka consumers set `tenant_id` on `EventEnvelope` by reading it from the event payload or from the source YAML config (for sources where all events belong to a single tenant).

---

### 9. Alert Output Producer

**Responsibility:** Publish a structured alert message to the `alerts.fired` Kafka topic.

**Message schema (Avro, registered in Schema Registry as `alerts.fired-value`):**

```json
{
  "tenantId": "string",
  "alertId": "string (UUID)",
  "ruleTemplateId": "string (UUID)",
  "ruleName": "string",
  "entityDimensionField": "string",
  "entityDimensionValue": "string",
  "matchedEventSnapshot": "string (JSON)",
  "aggregationSnapshot": "string (JSON)",
  "firedAt": "long (epoch millis)",
  "severity": "enum(INFO, LOW, MEDIUM, HIGH, CRITICAL)"
}
```

- `AlertOutputProducer` is called by `AlertManager` after a new `Alert` record is committed to PostgreSQL. The Kafka send is transactional (Kafka transactions enabled) to ensure at-least-once delivery without duplicates on retry.
- The topic has 12 partitions, keyed on `tenantId + ":" + entityDimensionValue`, so downstream consumers for the same entity receive ordered alerts.
- Downstream consumers (suspend player, send email, etc.) own all domain actions. The alerting platform publishes and does nothing else.

---

### 10. REST API

**Base path:** `/api/v1`

| Resource | Method | Path | Role |
|---|---|---|---|
| Templates | GET | `/templates` | GLOBAL_ADMIN |
| Templates | POST | `/templates` | GLOBAL_ADMIN |
| Templates | PUT | `/templates/{id}` | GLOBAL_ADMIN |
| Templates | POST | `/templates/{id}/publish` | GLOBAL_ADMIN |
| Templates | POST | `/templates/{id}/unpublish` | GLOBAL_ADMIN |
| Tenant rules | GET | `/tenant-rules` | TENANT_ADMIN, ANALYST |
| Tenant rules | POST | `/tenant-rules/{id}/enable` | TENANT_ADMIN |
| Tenant rules | POST | `/tenant-rules/{id}/disable` | TENANT_ADMIN |
| Tenant rules | PUT | `/tenant-rules/{id}/parameters` | TENANT_ADMIN |
| Alerts | GET | `/alerts` | TENANT_ADMIN, ANALYST |
| Alerts | POST | `/alerts/{id}/acknowledge` | ANALYST |
| Alerts | POST | `/alerts/{id}/resolve` | ANALYST |
| LLM authoring | POST | `/authoring/generate-cel` | GLOBAL_ADMIN, ANALYST |
| Event sources | GET | `/event-sources` | all authenticated |
| Event sources | GET | `/event-sources/{id}/schema` | all authenticated |
| Audit | GET | `/audit` | GLOBAL_ADMIN, TENANT_ADMIN |

- All endpoints return JSON. Error responses follow RFC 7807 Problem Details.
- Pagination via cursor (opaque `next` token in response) on all list endpoints.
- `GET /alerts` supports query params: `tenantId`, `ruleId`, `status`, `entityDimensionValue`, `from`, `to`.

---

### 11. React SPA

**Deployment:** Independently deployable static bundle (Nginx or CDN). Communicates with the backend exclusively via the REST API above. No backend SSR.

**Key screens:**

- **Rule Authoring:** Select event source → describe rule in plain English → loading state during LLM call → review screen showing generated CEL + plain-English summary side by side → inline CEL editor → save as DRAFT or submit for activation.
- **Template Library (GLOBAL_ADMIN):** Table of all templates, status, tenant publication count. Actions: publish, unpublish, archive.
- **Tenant Rule Manager (TENANT_ADMIN):** List of published rules, enabled/disabled toggle, parameter override form, audit log drawer.
- **Alert Dashboard:** Table with columns: rule name, entity, status, severity, fired at. Filter bar: tenant (GLOBAL_ADMIN only), rule, status, date range. Row actions: acknowledge, resolve. Row expand: matched event snapshot + aggregation snapshot in collapsible JSON viewer.
- **Event Source Browser (read-only):** List of registered sources with schema field table (name, type, example value). Used by analysts when writing rules.

**Auth:** Keycloak JS adapter handles SSO redirect, token refresh, and attaches `Authorization: Bearer <token>` to every API call. Role-based route guards hide screens not applicable to the current user's role.

---

## Testing Decisions

### Unit tests (isolated, no I/O)

The following modules contain logic dense enough to warrant thorough unit testing in isolation from infrastructure:

- **`CelEvaluator`** — Property-based tests covering: correct evaluation for each custom aggregation function stub, type error handling, missing-field behavior, program cache correctness across concurrent callers.
- **`AggregationQueryBuilder`** — Parameterized tests for each tier-selection branch. Assert generated SQL string for a given (sourceId, field, windowDays, tier) tuple.
- **`AlertManager` state machine** — Unit tests for each valid and invalid state transition. Deduplication logic tested with mock repository returning OPEN alert.
- **`LLMRuleAuthoringService` retry loop** — Mock the Anthropic HTTP client. Test: success on first attempt, success on second attempt after CEL validation failure, failure after three attempts returning structured error.
- **`TenantContextFilter`** — Test JWT extraction, missing-claim rejection, role mapping.
- **`EventEnvelope` normalization** — Test field extraction for various schema shapes, missing `entityDimensionField` fallback behavior.

### Integration tests (Spring context, real or embedded dependencies)

- **`ClickHouseProvisioner`** — Against a Testcontainers ClickHouse instance. Assert tables and MVs are created on first startup and the operation is idempotent on second startup.
- **`ClickHouseWriter` + `AggregationQueryLayer`** — Write synthetic events via `ClickHouseWriter`; query via `AggregationQueryBuilder`; assert returned values match expected aggregates for each retention tier.
- **`RuleEvaluationOrchestrator` end-to-end** — Testcontainers PostgreSQL + embedded Redis + Testcontainers ClickHouse. Seed rule templates and tenant instances; publish synthetic `EventEnvelope`; assert `AlertManager` creates the expected alert record and that a subsequent identical event is suppressed.
- **`AlertOutputProducer`** — Embedded Kafka (spring-kafka-test). Assert that a call to `AlertManager.fireAlert` results in a correctly structured message on `alerts.fired` within 5 seconds.
- **REST API (Spring `@WebMvcTest`)** — Controller-layer slice tests for auth enforcement (wrong role returns 403), tenant scoping (TENANT_ADMIN cannot see other tenant's alerts), and pagination correctness. No full Spring context.

### Contract tests

- **Schema Registry subjects** — Pact or Avro schema evolution tests asserting backward compatibility of `alerts.fired-value` schema across versions.
- **LLM prompt** — Snapshot tests for the prompt template, asserting that schema fields are correctly injected. The Anthropic API itself is mocked in CI.

### Load / performance tests (not part of CI, run pre-release)

- ClickHouse query latency under realistic cardinality: 10 M events per source, 5 tenants, 50 concurrent rule evaluations. Assert p99 aggregation query < 200 ms.
- CEL evaluation throughput: 1,000 compiled programs evaluated concurrently on a single JVM; assert no lock contention and throughput > 50 K evaluations/sec.

---

## Out of Scope

- **Domain actions on alert.** Suspending a player, sending emails, requesting ID documents — these are owned by downstream consumers of `alerts.fired`. The alerting platform fires the event and stops.
- **Migration of historical alert data.** The old system remains read-only for 90 days post-cutover. No alert history is imported into the new PostgreSQL database.
- **RabbitMQ becoming a first-class concern.** The RabbitMQ adapter exists to avoid breaking legacy publishers. It is not extended, not monitored with the same rigor as Kafka, and will be deprecated once legacy producers migrate.
- **UI for event source registration.** Registering a new event source is a YAML config file change by an operator. There is no UI for this operation.
- **Real-time streaming aggregation.** All aggregation is query-time against ClickHouse tiers. A Kafka Streams or Flink streaming aggregation pipeline is explicitly not part of this platform.
- **Self-service tenant onboarding.** Adding a new tenant (e.g., a sixth region) requires a GLOBAL_ADMIN to configure the tenant in Keycloak and seed the tenant record. There is no self-registration flow.
- **Rule simulation / backtesting.** Running a rule against historical events to preview how many alerts it would have fired is a valuable feature but is deferred to a future iteration.
- **Mobile or native clients.** The React SPA is the only supported frontend. Mobile apps, browser extensions, and CLI tooling are out of scope.
- **Alerting on alerting system health.** PagerDuty/Opsgenie integration for platform-level alerting (e.g., consumer lag breaching threshold) is owned by the observability team using the Prometheus metrics exposed by this platform. Routing rules are not in scope here.

---

## Further Notes

### Migration plan

- A one-time converter script reads all active `Template` + criterion records from the old PostgreSQL schema and produces draft `RuleTemplate` records (status = DRAFT) in the new schema. CEL expressions are generated by the LLM service using each template's human-readable description where available, or a structured description derived from criterion fields.
- GLOBAL_ADMINs review each draft in the new UI, test against the parallel-running event stream, and activate when satisfied.
- 2–4 weeks of parallel operation: both old and new systems consume the same Kafka topics. Alert output from both systems is compared daily (parity report) before the old system is switched off.
- Consul leader election from the old system is removed post-cutover. The new platform is active/active at the application layer; ClickHouse and PostgreSQL handle consistency.

### Scaling notes

- At 300 events/sec (~26 M/day), a single ClickHouse node with SSD storage handles ingest and query comfortably. Replication (1 shard, 2 replicas) is recommended for HA from launch.
- The rule evaluation path is stateless and horizontally scalable. Kafka consumer group partitioning distributes load naturally.
- Redis cluster mode is not required at launch given the short-lived, lossy nature of the evaluation cache. Standalone Redis with replica is sufficient.

### CEL custom function registration

The following custom functions are registered in the CEL environment at startup:

| Function | Arguments | Return type | Tier used |
|---|---|---|---|
| `agg_count(sourceId, windowDays)` | String, int | long | auto-selected |
| `agg_sum(sourceId, field, windowDays)` | String, String, int | double | auto-selected |
| `agg_max(sourceId, field, windowDays)` | String, String, int | double | raw or daily |
| `agg_distinct_count(sourceId, field, windowDays)` | String, String, int | long | raw |
| `agg_lifetime_sum(sourceId, field)` | String, String | double | monthly rollup sum |

Entity dimension value is implicit in the evaluation context (taken from `EventEnvelope.entityDimensionValue`) and injected into each ClickHouse query automatically.

### Open questions at PRD sign-off

1. Severity levels on alerts: should severity be declared on the `RuleTemplate` (static), computed from the CEL expression result (e.g., a numeric severity score), or set by the analyst at triage time? Current decision: static field on `RuleTemplate`, overridable per `TenantRuleInstance`.
2. ClickHouse `amount` field standardization: not all event sources use the field name `amount`. The YAML source config needs an explicit `aggregatableFields` list mapping canonical names to source field names. This needs finalization before the YAML schema is frozen.
3. Maximum CEL expression length / complexity limit: the compiler imposes no hard limit, but pathological expressions could cause slow compilation. A character limit (e.g., 4 KB) or AST depth limit should be defined before the authoring UI is built.
