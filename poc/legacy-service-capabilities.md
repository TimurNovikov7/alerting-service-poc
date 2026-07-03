# Anti-Fraud Service — Capabilities

## Table of Contents

1. [Overview](#overview)
2. [Rule Model](#rule-model)
3. [Triggers — How Rules Are Activated](#triggers--how-rules-are-activated)
4. [Rule Evaluation Flow](#rule-evaluation-flow)
5. [Criteria (Conditions)](#criteria-conditions)
   - [Payload / Profile Fields](#payload--profile-fields)
   - [Period Aggregates — Event-Log Queries](#period-aggregates--event-log-queries)
   - [Lifetime Running Totals](#lifetime-running-totals)
   - [Derived Metrics](#derived-metrics)
   - [Cross-Punter Matching](#cross-punter-matching)
   - [Partner-Level Criteria](#partner-level-criteria)
6. [Aggregates — How They Are Calculated and Stored](#aggregates--how-they-are-calculated-and-stored)
7. [Actions — What Happens When a Rule Fires](#actions--what-happens-when-a-rule-fires)
8. [Alert Deduplication and the `createCopies` Flag](#alert-deduplication-and-the-createcopies-flag)
9. [Rule Expression Capabilities and Limitations](#rule-expression-capabilities-and-limitations)
10. [Partner (Affiliate) Rules](#partner-affiliate-rules)
11. [Scheduled Rule Evaluation](#scheduled-rule-evaluation)
12. [Template Field Reference](#template-field-reference)

---

## Overview

`panbetfraudservice` is a real-time fraud detection service for the Panbet platform. It listens to player and financial events via Kafka, evaluates configurable rules (called **Templates** internally) against accumulated player data, and fires **Alerts** with automated remediation actions when all conditions of a rule are satisfied.

---

## Rule Model

The service uses the following terminology. Standard fraud-engine equivalents are noted in parentheses.

| Internal Term | Standard Term | Description |
|---|---|---|
| **Template** | Rule | The top-level unit. Binds triggers, conditions, and actions together. |
| **Trigger** | Activating event | The event(s) that cause the rule to be evaluated. |
| **Criterion / Criteria** | Condition(s) | The IF part of the rule. All must pass for the rule to fire. |
| **Action** | Action / THEN | What happens when the rule fires. |
| **Alert** | Rule match / incident | The record created when a rule fires. |
| **AlertItem** | Condition result | The evaluated result of one criterion, stored inside the Alert. |

A rule reads as: **"When** [trigger], **if** [all criteria match], **then** [execute actions]."

---

## Triggers — How Rules Are Activated

Each rule declares one or more triggers. A trigger is fired by the corresponding Kafka consumer after it stores the incoming event. Every `TriggerName` is statically typed as punter-scoped, partner-scoped, or both.

| TriggerName | Fired By | Punter | Partner |
|---|---|---|---|
| `CREATE_NEW_PUNTER` | Attribution lifecycle event | ✓ | ✓ |
| `EDIT_PUNTER` | Attribution lifecycle event | ✓ | — |
| `DEPOSIT` | Financial transfer (PURCHASE / FINISHED) | ✓ | ✓ |
| `FIRST_DEPOSIT` | Same deposit event, only on first deposit | — | ✓ |
| `WITHDRAW` | Financial transfer (withdrawal) | ✓ | ✓ |
| `WITHDRAW_REQUEST` | Financial transfer (withdrawal requested) | ✓ | — |
| `CHARGEBACK` | Financial transfer (chargeback) | ✓ | ✓ |
| `BET` | Sport bet (protobuf) | ✓ | ✓ |
| `FIRST_SPORT_BET` | Same bet event, only on first bet | — | ✓ |
| `FIRST_EXTERNAL_BET` | External bet event | — | ✓ |
| `SUCCESS_LOGIN` | Punter auth success event | ✓ | ✓ |
| `FAILED_LOGIN` | Punter auth failed event | ✓ | — |
| `PUNTER_STATE_CHANGED` | Punter state message | ✓ | — |
| `ADD_PAYMENT_SOURCE` | Attribution lifecycle event | ✓ | — |
| `CHANGE_EMAIL` | Attribution lifecycle event | ✓ | — |
| `CHANGE_CURRENCY` | Attribution lifecycle event | ✓ | — |
| `CHANGE_DEPOSIT_LIMITS` | Attribution lifecycle event | ✓ | — |
| `CHANGE_LOSS_LIMITS` | Attribution lifecycle event | ✓ | — |
| `CHANGE_TIME_OUT` | Attribution lifecycle event | ✓ | — |
| `ADD_TO_LINKED_ACCOUNT_BY_PHONE` | Linked account change event | ✓ | — |
| `EXTERNAL_CHECK` | External alert-generating event | — | — |
| `PERIODIC_PARTNER_CHECK` | Scheduled cron job (1st and 16th of month) | — | ✓ |

A rule can be linked to **multiple triggers**. It will be evaluated on every one of them.

**Example 1 — Monitor any large financial movement**

A rule checking `DEPOSIT_WITHDRAW_SUM > 50,000` with action `SEND_EMAILS` is linked to both `DEPOSIT` and `WITHDRAW`. It fires whenever either event occurs and the running lifetime total crosses the threshold — whichever transaction type pushes the punter over the limit triggers the alert.

**Example 2 — Suspicious new account with matching data**

A rule checking `MATCHING_PHONE_NUMBER` + `MATCHING_BIRTHDATE` is linked to both `CREATE_NEW_PUNTER` and `FIRST_DEPOSIT`. The intent is to catch a duplicate account at the earliest possible moment — either the instant the account is created (if another matching account already exists) or at first deposit (if the matching account was created after this one). With `createCopies = false` only one alert is produced regardless of which trigger fires first.

---

## Rule Evaluation Flow

```
Kafka Event
    │
    ▼
Kafka Consumer → Handler
    ├─ Save event to history table (AggregationService)
    ├─ Update lifetime running total (PunterTotalAmountService)
    └─ Fire trigger: TriggerService.triggerFor(punterId, triggerName)
            │
            ▼
    Load Trigger entity from DB (L2 Hibernate cache)
            │
            ▼
    ProcessingService.process(punterId, trigger, triggeredAt)
    ├─ Find active, non-deleted Templates linked to trigger
    ├─ Filter: time-of-day window (optional per template)
    ├─ Load PunterCondition (in-process cache → remote PPS on miss)
    ├─ Skip excluded punter types
    ├─ Filter: jurisdiction must match template's jurisdiction
    └─ Early skip: if createCopies=false, alert already exists,
                   and template has ≥N HARD criteria → skip entirely
            │
            ▼ [one task per template, executed in parallel via thread pool]
    TemplateProcessor.process(template, punterCondition, triggeredAt)
    ├─ Sort criteria: SIMPLE → MEDIUM → HARD  (fail-fast order)
    ├─ For each criterion:
    │   ├─ Check per-criterion jurisdiction skip override
    │   └─ Dispatch to CriterionProcessor for this CriterionType
    │       └─ Returns AlertItem(isMatched, description)
    ├─ Short-circuit: first non-matched criterion → return null (no alert)
    ├─ Process all MATCHING_* criteria as one batched remote search
    └─ All criteria matched? → return Alert object
            │
            ▼
    AlertService.createAlertIfNeeded(punterId, alert)
    ├─ Acquire pessimistic row-level lock on punterId
    ├─ Dedup check (createCopies flag)
    ├─ Advance deposit/withdraw "start point" for delta tracking
    ├─ ActionService.doActions(alert)
    └─ alertRepository.save(alert)
```

### Execution notes

- All templates matching the same trigger are evaluated **concurrently** in a fixed-size thread pool. Each trigger name group has its own pool.
- Criteria within a template are evaluated **sequentially**, with fail-fast: the first non-matched criterion stops evaluation immediately. Sorting by complexity (SIMPLE first) minimises unnecessary DB queries.
- MATCHING_* criteria are always processed **last**, as a single batched remote search call.

---

## Criteria (Conditions)

All criteria within a rule are connected by **implicit AND**. There is no OR, NOT, or grouping. Every criterion must match for the rule to fire.

Each criterion stores a `CriterionType` and type-specific parameters. Most also store:
- `period` — a `java.time.Duration` defining the lookback window for event-log queries (`null` = all time).
- `operationType` — comparison operator (`GREATER`, `LESS`, `EQUALS`, `GREATER_OR_EQUAL`, `LESS_OR_EQUAL`, string ops).

Criteria are classified by **processing complexity** which controls evaluation order:

| Complexity | Meaning |
|---|---|
| `SIMPLE` | Direct field read from the already-loaded PunterCondition. No DB or network calls. |
| `MEDIUM` | Cache lookup or single-row DB query. |
| `HARD` | Multi-row DB range query, cross-punter search, or external service call. |

---

### Payload / Profile Fields

These read directly from the punter's profile loaded at the start of processing. Cost: zero additional I/O.

| CriterionType | Checks |
|---|---|
| `PUNTER_REF` | Punter reference string |
| `NAME` | First name |
| `SURNAME` | Last name |
| `EMAIL` | Email address |
| `PHONE_NUMBER` | Phone number (digits only) |
| `PHONE_VERIFIED_VIA_SMS` | Whether phone was verified by SMS (boolean) |
| `POST_CODE` | Postal code |
| `BIRTH_DATE_FROM` | Birthdate ≥ date |
| `BIRTH_DATE_TO` | Birthdate ≤ date |
| `REGISTRATION_DATE_FROM` | Registration date ≥ date |
| `REGISTRATION_DATE_PERIOD` | Time since registration ≤ duration |
| `REGISTRATION_IP_ADDRESS` | IP address at registration |
| `REGISTRATION_SOURCE_ID` | Source/channel ID at registration |
| `COUNTRY_ID` | Country |
| `TOWN` | Town |
| `PUNTER_STATE_ID` | Current account state |
| `PUNTER_TYPE_ID` | Punter type classification |
| `CURRENCY_ID` | Account currency |
| `FIRST_DEPOSIT_DATE` | Date of first deposit |
| `FIRST_DEPOSIT_PERIOD` | Time since first deposit ≤ duration |
| `LOCALE_ID` | Account language/locale |
| `IDENTITY_CHECKED_MANUALLY` | Boolean: manually KYC-checked |
| `IDENTITY_CONFIRMED` | Boolean: KYC confirmed |
| `NON_GAMING_ACCOUNT` | Boolean: account is non-gaming |
| `SECRET_QUESTION` | Secret question code |
| `SECRET_ANSWER` | Secret answer (encrypted comparison) |

---

### Period Aggregates — Event-Log Queries

These query stored event history for the punter within a configurable time window (`[triggeredAt - period, triggeredAt]`). When `period` is `null`, all history is scanned (lifetime).

If the template has `createCopies = true`, the window additionally starts from the timestamp of the last alert fired for this template+punter combination, enabling "amount/count since last alert" semantics.

| CriterionType | Queries | Additional filters |
|---|---|---|
| `DEPOSIT` | Deposit history | Cash source type, card BIN, rejected flag, phone match |
| `WITHDRAW` | Withdrawal history (finished only) | Cash source type |
| `CHARGEBACK` | Chargeback history | — |
| `BET` | Bet history | Product type (sport/virtual/etc.), bet status (WIN/LOSE/OPEN/etc.) |
| `SUCCESS_LOGIN` | Successful login events | — |
| `FAILED_LOGIN` | Failed login events | — |
| `PUNTER_STATES_COUNT` | State change events | Target state, reason, previous state, previous reason |
| `EMAIL_CHANGES_COUNT` | Punter actions of type email change | — |
| `VPN_LOGIN` | Last login VPN flag | — |
| `IP_ADDRESS` | Last successful login IP | — |
| `USER_AGENT` | Last successful login user agent | — |
| `SOURCE_ID` | Last successful login source ID | — |
| `ORGANIC_TRAFFIC` | Whether punter has no affiliate link | — |
| `AFFILKA_BAD_PUNTER` | Whether punter is flagged in Affilka | — |
| `HAS_NO_FINISHED_WITHDRAW` | Whether punter has no finished withdrawal | — |

All `DEPOSIT`, `WITHDRAW`, `CHARGEBACK`, `BET` criteria check both **count** and optionally **total amount** (with comparison operator and currency conversion).

---

### Lifetime Running Totals

Pre-computed totals maintained as running sums, updated on every event. No period window — these reflect all-time values.

| CriterionType | Checks |
|---|---|
| `BALANCE` | Current wallet balance (live cache) |
| `DEPOSIT_WITHDRAW_SUM` | Sum of all deposits + all withdrawals |
| `AML_SPAIN_TOTAL_DEPOSIT` | Total deposit amount since last alert reset (AML Spain regulation) |
| `AML_SPAIN_TOTAL_WITHDRAW` | Total withdrawal amount since last alert reset (AML Spain regulation) |
| `PROFIT` | Net profit = withdrawals − deposits |

---

### Derived Metrics

| CriterionType | Formula |
|---|---|
| `TURNOVER` | `(bets amount / deposits amount) × 100` as a percentage, over a configurable period |

---

### Cross-Punter Matching

These criteria ask whether **another punter in the system** shares a field value with the triggering punter. They are used to detect duplicate accounts and identity fraud.

All `MATCHING_*` criteria present in a rule are collected and sent as **one combined search request** to the player search service. The search returns any punter (other than the triggering one) who matches all the specified fields simultaneously.

| CriterionType | Matches On |
|---|---|
| `MATCHING_NAME` | First name (fuzzy) |
| `MATCHING_SURNAME` | Last name (fuzzy) |
| `MATCHING_SURNAME_STRICT` | Last name (exact) |
| `MATCHING_EMAIL` | Email (fuzzy) |
| `MATCHING_EMAIL_STRICT` | Email (exact) |
| `MATCHING_PHONE_NUMBER` | Phone number (fuzzy) |
| `MATCHING_PHONE_NUMBER_STRICT` | Phone number (exact) |
| `MATCHING_BIRTHDATE` | Birthdate |
| `MATCHING_POSTCODE` | Postal code (fuzzy) |
| `MATCHING_POSTCODE_STRICT` | Postal code (exact) |
| `MATCHING_REGISTRATION_IP` | Registration IP address |
| `MATCHING_STATES` | Other punter must currently be in specified state(s) |
| `MATCHING_DATE_PERIOD` | Other punter must have registered within a duration relative to now |

**Example rule using matching:**
> "Fire when a newly depositing punter shares the same phone number **and** same birthdate with another account registered in the last 90 days."
>
> Criteria: `FIRST_DEPOSIT_PERIOD(7d)` + `MATCHING_PHONE_NUMBER` + `MATCHING_BIRTHDATE` + `MATCHING_DATE_PERIOD(90d)`

---

### Partner-Level Criteria

Used only in **partner templates** (affiliate fraud detection). They evaluate aggregate statistics across all punters belonging to one affiliate partner, not individual punter data.

| CriterionType | Checks |
|---|---|
| `PARTNER_PUNTERS_WITH_N_DEPOSITS` | Count of partner's punters who made ≥N deposits |
| `PARTNER_PUNTERS_WITH_N_BETS` | Count of partner's punters who placed ≥N bets |
| `PARTNER_PUNTERS_WITH_N_ACTIVE_DAYS` | Count of partner's punters active on ≥N days |
| `PARTNER_PUNTERS_WITH_SPECIFIC_PUNTER_TYPE` | Count with a specific punter type |
| `PARTNER_CHARGEBACKS` | Count of partner's punters with chargebacks |
| `PARTNER_PUNTERS_WITH_SAME_FIRST_DEPOSIT` | Count with identical first deposit amount |
| `PARTNER_PUNTERS_WITH_SAME_FIRST_SPORT_BET` | Count with identical first sport bet amount |
| `PARTNER_PUNTERS_WITH_SAME_FIRST_EXTERNAL_BET` | Count with identical first external bet amount |
| `PARTNER_PUNTERS_WITH_N_MINUTES_BETWEEN_FIRST_AND_SECOND_DEPOSIT` | Count where time between 1st and 2nd deposit ≤ N minutes |
| `PARTNER_PUNTERS_WITH_SAME_REG_IP_ADDRESS` | Count sharing registration IP |
| `PARTNER_PUNTERS_WITH_SAME_FINGERPRINT` | Count sharing device fingerprint |

---

## Aggregates — How They Are Calculated and Stored

The service has **no daily/monthly pre-aggregation buckets**. Instead it combines two storage strategies:

### Event-log tables (queryable by arbitrary time window)

Each event type is stored as an individual row in its **own dedicated table** when received. There is no shared event log. At rule evaluation time, the period window `[triggeredAt − criterion.period, triggeredAt]` is applied as a range query against the relevant table only.

| DB Table | Event Stored | Used By Criteria |
|---|---|---|
| `DEPOSIT_CONDITION` | One row per deposit (successful or rejected) | `DEPOSIT` |
| `WITHDRAW_CONDITION` | One row per finished withdrawal | `WITHDRAW` |
| `CHARGEBACK_CONDITION` | One row per chargeback | `CHARGEBACK` |
| `BET_CONDITION` | One row per bet (sport / external) | `BET` |
| `PUNTER_LOGIN_EVENT` | One row per login attempt | `SUCCESS_LOGIN`, `FAILED_LOGIN`, `VPN_LOGIN`, `IP_ADDRESS`, `USER_AGENT`, `SOURCE_ID` |
| `PUNTER_STATE_EVENT` | One row per state transition | `PUNTER_STATES_COUNT` |
| `PUNTER_ACTION` | One row per recorded punter action (e.g. email change) | `EMAIL_CHANGES_COUNT` |
| `first_bet` | One row per punter's first sport bet | `FIRST_SPORT_BET` trigger |
| `open_bet` | Currently open bets | internal tracking |
| `punter_partner_event` | Affiliate/partner link per punter | `ORGANIC_TRAFFIC`, partner processing |
| `punter_type_event` | Punter type classification history | `PUNTER_TYPE_ID` (historical) |

The period window drives how far back the query looks:

- `period = PT24H` → rolling 24-hour window (equivalent to "daily")
- `period = P30D` → rolling 30-day window (equivalent to "monthly")
- `period = null` → all history (equivalent to "lifetime")

The window start can be further constrained to `createdAt ≥ last alert time` when `createCopies = true` on the template.

### Running-total tables (continuously updated)

For lifetime deposit and withdrawal amounts, dedicated tables maintain a **running sum** updated on every event. No range query is needed — the current total is a single row lookup.

| DB Table | Holds |
|---|---|
| `PUNTER_DEPOSIT_SUM` | One row per punter — lifetime deposit total (`totalAmountDef`) |
| `PUNTER_WITHDRAW_SUM` | One row per punter — lifetime withdrawal total |

These are used by `DEPOSIT_WITHDRAW_SUM`, `PROFIT`, and `AML_SPAIN_TOTAL_*` criteria.

### Per-template delta tracking tables

For AML Spain compliance criteria and `createCopies`-enabled templates, additional tables track the running total value at the time of the last alert, enabling "amount since last alert" semantics.

| DB Table | Holds |
|---|---|
| `TEMPLATE_PUNTER_DEPOSIT_SUM` | Per-template, per-punter deposit running-total "start point" |
| `TEMPLATE_PUNTER_WITHDRAW_SUM` | Same for withdrawals |
| `TEMPLATE_PUNTER_PAYMENT_HANDLED_SUM` | Amount already processed/acknowledged by an AML-type rule |

The delta is computed as: `current running total − stored start point`. After each alert fires, the start point is advanced to the current total.

---

## Actions — What Happens When a Rule Fires

A rule can carry multiple actions; all are executed when the rule fires. Each action execution result is recorded in the Alert.

| ActionName | What It Does | Punter Rule | Partner Rule |
|---|---|---|---|
| `SUSPEND_PUNTER` | Suspends the punter's account | ✓ | — |
| `AUTO_WITHDRAW_OFF` | Disables automatic withdrawals (with a configured reason) | ✓ | — |
| `AUTO_WITHDRAW_TEMPORARY_OFF` | Temporarily disables auto withdrawals for a configured duration | ✓ | — |
| `WITHDRAW_OFF` | Disables withdrawals | ✓ | — |
| `DISABLE_PURCHASE` | Disables deposits/purchases | ✓ | — |
| `MARK_ACCOUNT_AS_NON_GAMING` | Marks account as non-gaming | ✓ | — |
| `SEND_ID_REQUEST` | Sends an identity document upload request to the punter | ✓ | — |
| `ACTIVATE_PUNTER_DOCUMENT_VERIFICATION` | Activates document verification workflow | ✓ | — |
| `SEND_EMAILS` | Sends email notification to configured `alertRecipients` | ✓ | ✓ |
| `SEND_MARIK` | Sends a message via Marik (internal messaging system) to configured `marikAlertRecipients` | ✓ | ✓ |
| `SEND_AFFILKA` | Sends a "bad punter" message to the Affilka affiliate platform via Kafka | ✓ | — |

Additionally, if `needCreatePunterDocument = true` on the template, a punter document is created regardless of action configuration.

---

## Alert Deduplication and the `createCopies` Flag

The `createCopies` flag on a Template controls whether the rule can fire multiple times for the same punter:

| `createCopies` | Behaviour |
|---|---|
| `false` (default) | The rule fires at most **once per punter**. Subsequent trigger evaluations are no-ops if an alert already exists. |
| `true` | The rule fires **every time** all criteria match. Each firing creates a new Alert. After firing, the deposit/withdraw "start point" is advanced so the next evaluation measures incremental amounts since the last alert. |

`createCopies = false` is also used as an **early-exit optimisation**: if the template has several expensive (HARD) criteria, the service checks for an existing alert before running any criteria at all, avoiding unnecessary DB queries.

---

## Rule Expression Capabilities and Limitations

| Capability | Supported |
|---|---|
| Multiple criteria per rule | ✓ |
| All criteria connected by AND | ✓ (implicit, always) |
| Configurable comparison operator per criterion (GT, LT, EQ, GTE, LTE) | ✓ |
| Arbitrary rolling time window per aggregate criterion | ✓ (any `java.time.Duration`) |
| Mix payload fields + period aggregates + lifetime totals in one rule | ✓ |
| Cross-punter field matching | ✓ |
| Fuzzy vs. strict matching variants | ✓ |
| Multiple triggers per rule | ✓ |
| Time-of-day window for rule activation | ✓ |
| Jurisdiction scoping per rule | ✓ |
| Per-criterion jurisdiction skip override | ✓ |
| OR between criteria | ✗ |
| NOT / negation | ✗ (except boolean criteria checking for `false`) |
| Criteria grouping / precedence | ✗ |
| Computed expressions across criteria (e.g. deposit − withdraw > X) | ✗ |
| Dynamic rule creation at runtime (no redeploy) | ✓ (rules are stored in DB, evaluated dynamically) |

---

## Partner (Affiliate) Rules

Partner templates evaluate fraud at the **affiliate level** rather than the individual punter level. Key differences:

- They only activate on triggers that have `partner = true` (e.g. `DEPOSIT`, `FIRST_DEPOSIT`, `PERIODIC_PARTNER_CHECK`).
- Processing groups punters by their affiliate partner tag, then evaluates the template's partner-level criteria against the aggregate statistics of each group.
- A `minCountPartnerPuntersWithFTD` threshold on the template gates evaluation: if the partner has fewer punters with a first-time deposit than this threshold in the check period, the template is skipped entirely.
- Partner criteria use a separate processor chain (`PartnerCriterionProcessor`) distinct from the punter criterion processors.
- Available actions for partner templates are restricted to `SEND_EMAILS` and `SEND_MARIK`.

---

## Scheduled Rule Evaluation

`PeriodicPartnerCheckJob` runs on a configurable cron schedule (default: 1st and 16th of each month at midnight). It fires `PERIODIC_PARTNER_CHECK` without a specific punter, causing the service to check all partners whose punters made first-time deposits in the preceding half-month window:

- Runs on 1st of month → window: 16th of previous month to 1st of current month
- Runs on 16th of month → window: 1st to 16th of current month

In a clustered deployment, the job only executes on the **inactive** node to avoid double-processing.

---

## Template Field Reference

| Field | Type | Description |
|---|---|---|
| `id` | Integer | Primary key |
| `name` | String | Human-readable rule name |
| `description` | String | Optional description |
| `jurisdictionId` | Integer | Rule applies only to punters in this jurisdiction |
| `active` | boolean | Whether the rule is active |
| `deleted` | boolean | Soft-delete flag |
| `triggers` | Set\<Trigger\> | Events that activate this rule |
| `criteria` | Set\<AbstractCriterion\> | Conditions — all must match |
| `actions` | Set\<TemplateAction\> | Actions executed when rule fires |
| `createCopies` | boolean | If false: fire once per punter. If true: fire every time criteria match. |
| `realtime` | boolean | Whether the rule is processed in real-time (vs. batch) |
| `useTimeRange` | boolean | Whether to restrict firing to a time-of-day window |
| `timeFrom` / `timeTo` | LocalDateTime | Time-of-day window boundaries (when `useTimeRange = true`) |
| `timeZoneId` | String | Timezone for the time-of-day window (e.g. `+03:00`) |
| `partnerTemplate` | boolean | If true: partner/affiliate rule; if false: punter rule |
| `minCountPartnerPuntersWithFTD` | int | Minimum number of partner punters with FTD required before evaluating partner rules |
| `needCreatePunterDocument` | boolean | If true: automatically create a punter document on alert |
| `markImportantPunterDocument` | boolean | If true: mark the created document as important |
| `addConditionsToAlert` | boolean | If true: include evaluated condition details in the alert record |
| `alertRecipients` | List\<String\> | Email addresses for `SEND_EMAILS` action |
| `marikAlertRecipients` | List\<String\> | Marik recipients for `SEND_MARIK` action |
| `createDate` | LocalDate | Date the rule was created |
