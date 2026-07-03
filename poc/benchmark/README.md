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
pip install -r requirements.txt   # clickhouse-connect, numpy
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

Runs 13 queries covering all three sources and all tiers (raw, mv_daily, mv_monthly) in two modes:

- **Single-threaded:** 200 reps per query
- **Concurrent:** 50 threads × 20 rounds (1 000 samples per query)

Reports p50 / p95 / p99 / max and flags any p99 > 200 ms as an SLO violation. Results are saved to `phase1_results.csv`.

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
