# Phase 1 — ClickHouse Query Latency Benchmark

Standalone benchmark that seeds ClickHouse with production-scale synthetic data and measures aggregation query latency for the EventMonitor use case.

## Data Profile

| Source | Rate | Rows (30-day retention) | Key dimensions |
|---|---|---|---|
| `punter_login` | 10 events/sec | ~26 M | `dim_punter_id` |
| `withdrawal` | 50 events/sec | ~130 M | `dim_punter_id` |
| `external_bet` | 240 events/sec | ~622 M | `dim_punter_id`, `dim_bet_source` |

- **Punters:** 100 000 unique IDs
- **Retention:** 30 days
- **Total rows:** ~778 M

## Table Layout

All tables use a `bm_` prefix and never conflict with live PoC tables.

| Table | Engine | Used for |
|---|---|---|
| `bm_events_<source>` | MergeTree | Raw events; queries with window ≤ 1 day |
| `bm_mv_daily_<source>` | SummingMergeTree | Pre-aggregated by day; queries 2–30 days |
| `bm_mv_monthly_<source>` | SummingMergeTree | Pre-aggregated by month; lifetime queries |

## Prerequisites

```bash
pip install clickhouse-connect>=0.7.0 numpy>=1.24.0   # clickhouse-connect, numpy
```

ClickHouse must be reachable on `localhost:8123`. Start it with:

```bash
# from poc/
docker compose up clickhouse -d

# verify
curl http://localhost:8123/ping   # → Ok.
```

## Commands

### Seed — populate benchmark tables (~26–30 min)

Creates all `bm_*` tables and fills them with synthetic data at the production-scale profile above. After inserting raw rows, materializes the daily and monthly rollup tables via `INSERT … SELECT … GROUP BY`.

```bash
python phase1.py --seed
```

### Or Seed a single source only

```bash
python phase1.py --seed --sources external_bet
```

### Status — check what was seeded

Reads `system.parts` and prints row counts and compressed on-disk sizes for every `bm_*` table. Use this to verify seeding completed correctly.

```bash
python phase1.py --status
```

### Bench — run query latency benchmark (~5 min)

Runs 15 queries covering all three sources and all tiers (raw, mv_daily, mv_monthly) in two modes:

- **Single-threaded:** 1 000 reps per query
- **Concurrent:** 50 threads × 100 rounds (5 000 samples per query)

Each rep queries a punter ID sampled uniformly at random from the full 100 000-punter population. Reports p50 / p95 / p99 / max and flags any p99 > 200 ms as an SLO violation. Results are saved to `phase1_results.csv`.

```bash
python phase1.py --bench
```


### Drop all benchmark tables

```bash
python phase1.py --drop
```

## Optional flags

| Flag | Default | Description |
|---|---|---|
| `--host` | `localhost` | ClickHouse host |
| `--port` | `8123` | ClickHouse HTTP port |
| `--sources` | all three | Limit seed/bench to specific sources |
| `--output` | `phase1_results.csv` | CSV output path for bench results |

## Results

Snapshot from `phase1_results.csv` (2026-07-03), full data profile, all three sources, default bench settings (1 000 single-threaded reps / 5 000 concurrent samples per query).

### Environment

| | |
|---|---|
| Host | Apple M4 Pro, 12 cores, 24 GB RAM, macOS 15.7.7 (arm64) |
| ClickHouse container (Docker) | 8 CPUs / 15.6 GiB memory allocated |
| ClickHouse server | 24.3.18.7 |
| `clickhouse-connect` (client) | 0.15.1 |
| Python | 3.9.6 |

Results are only comparable across runs on matching hardware/resource allocation — in particular, the Docker memory limit above (× ClickHouse's default 90% `max_server_memory_usage_to_ram_ratio`) is what capped total query memory at ~14 GiB during this benchmark.

### Summary

- **All 15 queries meet the 200 ms p99 SLO**, in both single-threaded and concurrent modes.
- **Baseline latency floor is ~4-8 ms single-threaded**, dominated by network round-trip and query planning rather than data volume — even raw-table point lookups against hundreds of millions of rows land here, because every query filters on `dim_punter_id`, the first column of each table's primary index, so ClickHouse touches only that punter's granules.
- **Concurrency roughly triples typical latency** (e.g. `punter_login | count | 1d | raw`: 3.8 ms → 14.0 ms p50) as 50 simultaneous queries genuinely compete for server CPU/threads — expected and still well within budget.
- **JSON extraction on raw payloads is the main latency driver**, not the raw/mv_daily/mv_monthly tier by itself. The heaviest query in the catalogue, `external_bet | sum(betAmount) | 7d | raw` — which does a nested `JSONExtractRaw` + `JSONExtractFloat` per row — is the slowest overall (conc p99 = 137.3 ms, max = 158.7 ms), closest to the SLO ceiling and the one query worth watching if traffic grows.
- **Rollup tables (`mv_daily`/`mv_monthly`) consistently beat their raw-table equivalents** on `sum`/`lifetime_sum` queries, since they read pre-aggregated typed columns instead of parsing JSON on every row (e.g. `withdrawal | lifetime_sum | mv_monthly` conc p50 = 11.0 ms vs. `withdrawal | sum(amount) | 7d | raw` conc p50 = 42.0 ms).

### Results — single-threaded (1 000 queries)

| Query | Tier/Table | n | Mean | p50 | p95 | p99 | Max | SLO (p99<200ms) |
|---|------------|---:|---:|---:|---:|---:|---:|:---:|
| punter_login \| count \| 1d | raw        | 1000 | 3.9ms | 3.8ms | 5.3ms | 6.6ms | 10.3ms | ✓ |
| punter_login \| count \| 7d | mv_daily   | 1000 | 3.9ms | 3.9ms | 4.6ms | 4.9ms | 6.9ms | ✓ |
| punter_login \| count \| 30d | mv_daily   | 1000 | 3.9ms | 4.0ms | 4.7ms | 5.4ms | 7.4ms | ✓ |
| withdrawal \| count \| 1d | raw        | 1000 | 3.8ms | 3.7ms | 4.9ms | 5.7ms | 6.5ms | ✓ |
| withdrawal \| count \| 7d | mv_daily   | 1000 | 4.1ms | 4.1ms | 4.8ms | 5.6ms | 7.8ms | ✓ |
| withdrawal \| sum(amount) \| 7d | raw        | 1000 | 6.2ms | 5.8ms | 8.0ms | 13.2ms | 73.7ms | ✓ |
| withdrawal \| sum(amount) \| 30d | raw        | 1000 | 5.1ms | 5.1ms | 5.9ms | 6.8ms | 8.9ms | ✓ |
| withdrawal \| lifetime_sum | mv_monthly | 1000 | 3.5ms | 3.6ms | 4.3ms | 4.8ms | 10.7ms | ✓ |
| external_bet \| count \| 1d | raw        | 1000 | 4.4ms | 4.4ms | 5.2ms | 5.7ms | 6.4ms | ✓ |
| external_bet \| count \| 7d | mv_daily   | 1000 | 4.6ms | 4.6ms | 5.4ms | 6.1ms | 16.0ms | ✓ |
| external_bet \| count \| 30d | mv_daily   | 1000 | 4.7ms | 4.7ms | 5.4ms | 6.8ms | 9.7ms | ✓ |
| external_bet \| count (compound) \| 7d | mv_daily   | 1000 | 5.1ms | 5.1ms | 6.0ms | 6.8ms | 9.5ms | ✓ |
| external_bet \| sum(betAmount) \| 1d | raw        | 1000 | 5.1ms | 5.1ms | 6.0ms | 6.7ms | 10.9ms | ✓ |
| external_bet \| sum(betAmount) \| 7d | raw        | 1000 | 7.3ms | 7.2ms | 8.9ms | 10.3ms | 13.4ms | ✓ |
| external_bet \| lifetime_sum | mv_monthly | 1000 | 3.8ms | 3.8ms | 4.5ms | 5.2ms | 14.0ms | ✓ |

### Results — concurrent (50 threads × 100 rounds = 5 000 queries)

| Query | Tier/Table | n | Mean | p50 | p95 | p99 | Max | SLO (p99<200ms) |
|---|------------|---:|---:|---:|---:|---:|---:|:---:|
| punter_login \| count \| 1d | raw        | 5000 | 14.7ms | 14.0ms | 24.0ms | 28.8ms | 59.2ms | ✓ |
| punter_login \| count \| 7d | mv_daily   | 5000 | 17.2ms | 16.7ms | 27.6ms | 32.7ms | 43.8ms | ✓ |
| punter_login \| count \| 30d | mv_daily   | 5000 | 17.2ms | 16.7ms | 27.5ms | 32.7ms | 38.9ms | ✓ |
| withdrawal \| count \| 1d | raw        | 5000 | 12.0ms | 11.3ms | 20.6ms | 24.2ms | 31.7ms | ✓ |
| withdrawal \| count \| 7d | mv_daily   | 5000 | 19.5ms | 18.3ms | 32.1ms | 48.7ms | 122.6ms | ✓ |
| withdrawal \| sum(amount) \| 7d | raw        | 5000 | 40.8ms | 42.0ms | 59.0ms | 65.7ms | 75.8ms | ✓ |
| withdrawal \| sum(amount) \| 30d | raw        | 5000 | 39.4ms | 40.3ms | 56.4ms | 62.4ms | 73.3ms | ✓ |
| withdrawal \| lifetime_sum | mv_monthly | 5000 | 11.8ms | 11.0ms | 20.7ms | 24.6ms | 32.2ms | ✓ |
| external_bet \| count \| 1d | raw        | 5000 | 25.5ms | 25.6ms | 38.4ms | 44.9ms | 55.3ms | ✓ |
| external_bet \| count \| 7d | mv_daily   | 5000 | 29.6ms | 29.3ms | 45.9ms | 51.6ms | 63.1ms | ✓ |
| external_bet \| count \| 30d | mv_daily   | 5000 | 29.9ms | 30.2ms | 44.7ms | 50.6ms | 62.7ms | ✓ |
| external_bet \| count (compound) \| 7d | mv_daily   | 5000 | 33.4ms | 33.8ms | 49.7ms | 56.5ms | 68.0ms | ✓ |
| external_bet \| sum(betAmount) \| 1d | raw        | 5000 | 35.9ms | 36.3ms | 53.3ms | 61.0ms | 77.4ms | ✓ |
| external_bet \| sum(betAmount) \| 7d | raw        | 5000 | 86.1ms | 89.0ms | 120.8ms | 137.3ms | 158.7ms | ✓ |
| external_bet \| lifetime_sum | mv_monthly | 5000 | 13.9ms | 13.2ms | 23.4ms | 28.7ms | 36.7ms | ✓ |

---

# Phase 2 — End-to-End Pipeline Latency Benchmark

Measures wall-clock time from the moment the first triggering event is committed to Kafka until the alert appears in `GET /api/v1/alerts`.

**Full pipeline under test:**
```
phase2.py → Kafka → KafkaConsumerManager → CHReadinessGate
          → RuleEvaluationOrchestrator → AlertManager → PostgreSQL
          ← GET /api/v1/alerts ← phase2.py
```

**Timing definition:**
- `t_produce` — `datetime.now()` just before the first `producer.send()` in a scenario run
- `t_detect`  — `datetime.now()` when the polling loop first sees the alert
- `latency`   = `t_detect − t_produce` (includes ≤ 200 ms poll jitter)

**Note:** the benchmark only *detects* alerts during a scenario's trials — it deliberately does not resolve them mid-run. Resolving immediately after detection raced with the backend still evaluating the remaining events in the same trigger burst, which could make `AlertManager`'s open-alert dedup miss and create spurious duplicate alerts (a real backend-side gap, not a benchmark bug).

Since punter IDs restart from `BM2_PUNTER_BASE+1` on every run, an uncleaned previous run would otherwise leave an `OPEN` alert with the exact same `(ruleId, entityDimensionValue)` a later run produces, which `run_scenario()`'s polling loop could match immediately — a false-positive near-zero latency, not a real measurement. Two layers guard against this: `main()` calls `cleanup()` automatically before each scenario starts, and `run_scenario()` additionally checks a candidate alert's `firedAt` is after this trial's own `t_produce` before accepting it as a match. Run `--cleanup` once more after the whole `--bench` run finishes, to resolve the last scenario's alerts too.

## Scenarios

| Scenario key | Rule | Topic | Events per scenario run | Trigger condition |
|---|---|---|---|---|
| `large_withdrawal` | Large Withdrawal Alert | `withdrawal` | 1 | `payload.amount > 500` — no aggregation |
| `login_frequency` | Suspicious Login Frequency | `punter-auth-success-login` | 4 | `agg_count > 3` in 1 day |
| `high_bet_volume` | High Daily Betting Volume | `ebs_bets` | 3 | `agg_sum(betAmount) > 500` in 1 day |
| `concentrated_source` | Concentrated Source Betting | `ebs_bets` | 21 | `agg_count(punter+source) > 20` in 7 days |

`CHReadinessGate` always waits for ClickHouse to catch up on the triggering source itself, for every scenario including `large_withdrawal` — there's no exemption for non-aggregating rules. What `large_withdrawal` actually skips is the *cross-source* `END_OFFSET` wait and the ClickHouse aggregation query itself, since its rule needs no `agg_*()` call. The other three scenarios pay both the triggering-source wait and the aggregation query on top.

## Prerequisites

Full stack must be running:

```bash
# from poc/
docker compose up -d
```

Verify all services are healthy:

```bash
curl http://localhost:8080/api/v1/rules   # → JSON array with 4 rules
curl http://localhost:8123/ping           # → Ok.
```

Install dependencies (if not already):

```bash
pip install clickhouse-connect>=0.7.0 numpy>=1.24.0 kafka-python<3.0.0 requests>=2.28.0 
```

## Commands

### Run benchmark (default: 30 sequential + 10×3 concurrent scenario runs per scenario)

```bash
python phase2.py --bench
```

### Run with production-rate background load (+300 events/s)

```bash
python phase2.py --bench --background-rate 300
```

Background-load punters (random IDs below `BM2_PUNTER_BASE`) can trip a rule purely by chance. `AutoResolutionJob` can't clean these up on any useful timescale — it only resolves an alert once the rule's own aggregation window ages out, up to 1-7 real days for these rules. So whenever `--background-rate > 0`, the benchmark runs a second background thread that sweeps and resolves all open alerts every 5s for the duration of the run, independent of the per-scenario/per-round cleanup.

### Run a single scenario only

```bash
python phase2.py --bench --scenarios large_withdrawal
python phase2.py --bench --scenarios login_frequency high_bet_volume
```

### Custom scenario run counts

```bash
python phase2.py --bench --scenario-runs 50 --concurrent 20 --conc-rounds 5
```

### Resolve all open alerts (run after benchmarking — the benchmark never auto-resolves; this clears everything, including alerts from `--background-rate` traffic, not just BM2 scenario alerts)

```bash
python phase2.py --cleanup
```

### Cleanup + bench in one go

```bash
python phase2.py --cleanup --bench
```

## Optional flags

| Flag | Default | Description |
|---|---|---|
| `--backend` | `http://localhost:8080` | Backend base URL |
| `--kafka` | `localhost:29092` | Kafka bootstrap servers |
| `--scenario-runs` | `30` | Sequential scenario runs per scenario |
| `--concurrent` | `10` | Concurrent workers per round |
| `--conc-rounds` | `3` | Concurrent rounds (total = workers × rounds) |
| `--sequential-only` | off | Skip the concurrent phase entirely, run sequential trials only |
| `--background-rate` | `0` | Background events/sec to simulate pipeline load |
| `--scenarios` | all four | Limit to specific scenario keys |
| `--output` | `phase2_results.csv` | CSV output path |

## Interpreting results

- **`large_withdrawal` sequential latency** — baseline pipeline cost: Kafka consume + inline rule evaluation + PostgreSQL write. Should be well under 1 s.
- **Aggregation scenario overhead** — difference between aggregation scenarios and `large_withdrawal` is the CHReadinessGate wait time + ClickHouse query time.
- **Sequential vs concurrent** — how serialised backend processing (single consumer thread per Kafka topic) affects tail latency under burst load.
- **Timeouts** — scenario runs that exceeded 120 s. Any timeout is a pipeline health signal.

## Results

Live-testing snapshot (2026-07-06), full stack running locally (see [Phase 1 Environment](#environment) for hardware/software details — same machine and stack). All runs used the default `kafka_flush_interval_ms=3000` / 6 Kafka partitions per topic setup described above.

### `--bench --background-rate {100,200,300}` — all 4 scenarios, sequential + concurrent

**100 events/s** — `11,258 events sent over 112.6s ≈ 100 events/s actual`

| Rule | Mode | p50 | p95 | p99 | max | Timeouts |
|---|---|---:|---:|---:|---:|---:|
| Large Withdrawal Alert | sequential | 3482ms | 3598ms | 3598ms | 3598ms | 0 |
| Large Withdrawal Alert | concurrent | 3379ms | 3481ms | 3635ms | 3635ms | 0 |
| Suspicious Login Frequency | sequential | 3667ms | 4160ms | 4160ms | 4160ms | 0 |
| Suspicious Login Frequency | concurrent | 3759ms | 4379ms | 6471ms | 6471ms | 0 |
| High Daily Betting Volume | sequential | 1744ms | 2151ms | 2151ms | 2151ms | 0 |
| High Daily Betting Volume | concurrent | 2046ms | 3146ms | 3346ms | 3346ms | 0 |
| Concentrated Source Betting | sequential | 2098ms | 2165ms | 2165ms | 2165ms | 0 |
| Concentrated Source Betting | concurrent | 2711ms | 4673ms | 5810ms | 5810ms | 0 |

**200 events/s** — `30,014 events sent over 150.1s ≈ 200 events/s actual`

| Rule | Mode | p50 | p95 | p99 | max | Timeouts |
|---|---|---:|---:|---:|---:|---:|
| Large Withdrawal Alert | sequential | 3431ms | 3595ms | 3595ms | 3595ms | 0 |
| Large Withdrawal Alert | concurrent | 2931ms | 3651ms | 3750ms | 3750ms | 0 |
| Suspicious Login Frequency | sequential | 4195ms | 4982ms | 4982ms | 4982ms | 0 |
| Suspicious Login Frequency | concurrent | 4304ms | 6877ms | 6927ms | 6927ms | 0 |
| High Daily Betting Volume | sequential | 2818ms | 3328ms | 3328ms | 3328ms | 0 |
| High Daily Betting Volume | concurrent | 3399ms | 3862ms | 3905ms | 3905ms | 0 |
| Concentrated Source Betting | sequential | 2722ms | 3311ms | 3311ms | 3311ms | 0 |
| Concentrated Source Betting | concurrent | 4660ms | 8276ms | 9593ms | 9593ms | 0 |

**300 events/s** — `50,533 events sent over 168.4s ≈ 300 events/s actual`

| Rule | Mode | p50 | p95 | p99 | max | Timeouts |
|---|---|---:|---:|---:|---:|---:|
| Large Withdrawal Alert | sequential | 2135ms | 2994ms | 2994ms | 2994ms | 0 |
| Large Withdrawal Alert | concurrent | 1950ms | 2572ms | 2593ms | 2593ms | 0 |
| Suspicious Login Frequency | sequential | 2147ms | 2556ms | 2556ms | 2556ms | 0 |
| Suspicious Login Frequency | concurrent | 2879ms | 3931ms | 3932ms | 3932ms | 0 |
| High Daily Betting Volume | sequential | 2548ms | 3656ms | 3656ms | 3656ms | 0 |
| High Daily Betting Volume | concurrent | 3919ms | 4550ms | 4553ms | 4553ms | 0 |
| Concentrated Source Betting | sequential | 3886ms | 4063ms | 4063ms | 4063ms | 0 |
| Concentrated Source Betting | concurrent | 6203ms | 9255ms | 9707ms | 9707ms | 0 |

**Reading these three runs together:** no timeouts at any load level, but they're not directly comparable apples-to-apples run over run — this is live testing against a shared, continuously-changing dataset (background traffic keeps accumulating history across runs — see the per-event SQL analysis below), so absolute latency drifts for reasons beyond just the background rate itself. The one **consistent** trend across all three runs is `Concentrated Source Betting`'s concurrent tail growing with load (p99/max: 5810ms → 9593ms → 9707ms at 100/200/300 events/s) — expected, since it's the most expensive rule per event (7-day window via `mv_daily_external_bet`) and shares `ebs_bets` with a second rule (`High Daily Betting Volume`), doubling that topic's per-event ClickHouse query cost versus `punter_login`'s single rule.

### Per-rule throughput ceiling (isolated via `phase3.py`, no scenario/alert-detection noise)

Using `phase3.py` to generate pure background load for one rule's topic at a time (bypassing `phase2.py`'s scenario trials and alert polling entirely) isolates each rule's actual sustainable ClickHouse query throughput:

| Rule | Command | Sustainable query rate | Rate where Kafka latency starts increasing |
|---|---|---:|---:|
| Suspicious Login Frequency | `phase3.py --rate 300 --event-type punter_login --duration 120` | `events_punter_login` ≈ 18,149/min | `--rate 500` → 23,770/min, latency increasing |
| High Daily Betting Volume | `phase3.py --rate 300 --event-type ebs_bets --duration 180` | `events_external_bet` ≈ 18,104/min | `--rate 500` → 20,092/min, latency increasing |
| Concentrated Source Betting | `phase3.py --rate 300 --event-type ebs_bets --duration 180` | `mv_daily_external_bet` ≈ 18,035/min | `--rate 500` → 24,783/min, latency increasing |

At `--rate 300` for any single rule in isolation, Kafka consumer lag stays flat (latency "stays within 300") — each rule individually has headroom well above 300 events/s. The ceiling only starts to bite around `--rate 500`, where Kafka latency begins climbing — the first sign of the consumer falling behind, before it becomes an unbounded runaway backlog.

**Combined — all 3 rules simultaneously** (`phase3.py --rate 300 --event-type ebs_bets punter_login --duration 180`, i.e. 300 events/s split across the two topics, `ebs_bets` carrying both bet rules):

| minute | `events_external_bet` | `mv_daily_external_bet` | `events_punter_login` |
|---|---:|---:|---:|
| 2026-07-06 21:26 | 8,463 | 8,465 | 8,811 |
| 2026-07-06 21:27 | 8,784 | 8,784 | 8,710 |

Per-topic query rates roughly halve compared to the isolated single-topic runs above (≈18,000/min → ≈8,500/min), consistent with splitting the same 300 events/s total across two topics instead of dedicating it fully to one — confirming the earlier finding that `ebs_bets`'s two stacked rules are the binding constraint on total system throughput, not `punter_login`'s single rule.

