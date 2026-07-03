#!/usr/bin/env python3
"""
EventMonitor — Phase 1 ClickHouse Benchmark
Query latency at production-scale data volumes.

Data profile
  Punters      : 100 000 unique IDs
  Retention    : 30 days
  Sources      : punter_login 10/s  |  withdrawal 50/s  |  external_bet 240/s
  Row counts   : ~26 M               |  ~130 M            |  ~622 M
  Seeding time : ~1 min              |  ~4 min            |  ~21 min (HTTP INSERT)

All tables are created with a bm_ prefix so they never conflict with live PoC tables.
MVs are populated via INSERT…SELECT after raw seeding (faster than trigger-based approach).

Usage
  docker compose up clickhouse -d                  # start clickhouse
  curl http://localhost:8123/ping                  # verify clickhosue started
  pip install -r requirements.txt
  python phase1.py --seed                          # ~30 min total; run once
  python phase1.py --status                        # row counts + compressed sizes
  python phase1.py --bench                         # ~5 min; repeatable
  python phase1.py --drop                          # wipe all bm_* tables
  python phase1.py --seed --sources external_bet   # seed one source only
  python phase1.py --materialize-only --sources external_bet   # retry a failed rollup, no raw re-insert
  python phase1.py --host 192.168.1.10 --port 8123
"""

from __future__ import annotations

import argparse
import csv
import math
import random
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any

import clickhouse_connect
import numpy as np

# ── benchmark parameters ─────────────────────────────────────────────────────

NUM_PUNTERS    = 100_000
RETENTION_DAYS = 30
INSERT_BATCH   = 500_000     # rows per INSERT call (balance memory vs round-trips)
PAYLOAD_POOL   = 50_000      # pre-built payload strings; sampled via numpy indexing

BENCH_REPS         = 200     # single-threaded reps per query
CONCURRENT_WORKERS = 50      # threads for concurrency test
CONCURRENT_REPS    = 20      # rounds of 50-thread bursts (= 1 000 total samples)

BET_SOURCES = ["WEB", "MOBILE", "API"]
BET_TYPES   = ["SINGLE", "ACCUMULATOR", "SYSTEM"]
COUNTRIES   = ["UK", "DE", "FR", "ES", "IT", "PL", "NL", "SE", "NO", "AU"]

# Populated once at startup in _build_payload_pools()
_POOLS: dict[str, np.ndarray] = {}

# ── source definitions ────────────────────────────────────────────────────────
# extra_dims   — dimension columns beyond dim_punter_id
# event_types  — values for the event_type column
# amount_expr  — ClickHouse SQL to extract the numeric amount from payload
#                (None = no numeric payload field)

SOURCES: dict[str, dict] = {
    "punter_login": {
        "rate":        10,
        "extra_dims":  [],
        "event_types": ["LOGIN"],
        "amount_expr": None,
    },
    "withdrawal": {
        "rate":        50,
        "extra_dims":  [],
        "event_types": ["withdrawal"],
        "amount_expr": "JSONExtractFloat(payload, 'amount')",
    },
    "external_bet": {
        "rate":        240,
        "extra_dims":  ["bet_source"],
        "event_types": BET_TYPES,
        "amount_expr": "JSONExtractFloat(JSONExtractRaw(payload, 'betAmount'), 'amount')",
    },
}


def _build_payload_pools():
    rng = np.random
    amounts = np.round(rng.uniform(5.0, 5_000.0, PAYLOAD_POOL), 2)

    # punter_login: {"countryCode":"UK","authMethod":"PASSWORD"}
    countries = rng.choice(COUNTRIES, PAYLOAD_POOL)
    methods   = rng.choice(["PASSWORD", "BIOMETRIC", "SMS_OTP"], PAYLOAD_POOL)
    _POOLS["punter_login"] = np.array(
        [f'{{"countryCode":"{c}","authMethod":"{m}"}}' for c, m in zip(countries, methods)]
    )

    # withdrawal: {"amount":500.0,"currency":"USD"}
    currencies = rng.choice(["USD", "EUR", "GBP"], PAYLOAD_POOL)
    _POOLS["withdrawal"] = np.array(
        [f'{{"amount":{a},"currency":"{c}"}}' for a, c in zip(amounts, currencies)]
    )

    # external_bet: {"betAmount":{"amount":120.5},"betState":"OPEN"}
    states = rng.choice(["OPEN", "WON", "LOST"], PAYLOAD_POOL)
    _POOLS["external_bet"] = np.array(
        [f'{{"betAmount":{{"amount":{a}}},"betState":"{s}"}}' for a, s in zip(amounts, states)]
    )


# ── table names ───────────────────────────────────────────────────────────────

def raw_t(s: str)     -> str: return f"bm_events_{s}"
def daily_t(s: str)   -> str: return f"bm_mv_daily_{s}"
def monthly_t(s: str) -> str: return f"bm_mv_monthly_{s}"


# ── DDL ───────────────────────────────────────────────────────────────────────

def provision_tables(client, source_id: str, cfg: dict):
    dims        = cfg["extra_dims"]
    extra_cols  = "".join(f"\n    dim_{d}    String," for d in dims)
    order_extra = (", ".join(f"dim_{d}" for d in dims) + ", ") if dims else ""

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {raw_t(source_id)} (
            source_id     String,
            dim_punter_id String,{extra_cols}
            event_type    String,
            occurred_at   DateTime,
            payload       String
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(occurred_at)
        ORDER BY (dim_punter_id, {order_extra}occurred_at)
        TTL occurred_at + INTERVAL {RETENTION_DAYS} DAY
    """)

    for table, time_col in [(daily_t(source_id), "day"), (monthly_t(source_id), "month")]:
        ttl = f"TTL {time_col} + INTERVAL {RETENTION_DAYS} DAY\n        " if time_col == "day" else ""
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                dim_punter_id String,{extra_cols}
                event_type    String,
                {time_col}    Date,
                event_count   UInt64,
                total_amount  Float64
            ) ENGINE = SummingMergeTree
            PARTITION BY toYYYYMM({time_col})
            ORDER BY (dim_punter_id, {order_extra}event_type, {time_col})
            {ttl}
        """)

    print(f"    tables ready: {raw_t(source_id)}, {daily_t(source_id)}, {monthly_t(source_id)}")


# ── seeding ───────────────────────────────────────────────────────────────────

def seed_source(client, source_id: str, cfg: dict):
    total_rows  = cfg["rate"] * 86_400 * RETENTION_DAYS
    n_batches   = math.ceil(total_rows / INSERT_BATCH)
    window_sec  = RETENTION_DAYS * 86_400
    base_epoch  = int((datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).timestamp())
    dims        = cfg["extra_dims"]
    payload_pool = _POOLS[source_id]

    col_names = (
        ["source_id", "dim_punter_id"]
        + [f"dim_{d}" for d in dims]
        + ["event_type", "occurred_at", "payload"]
    )

    print(f"\n  {source_id}:  {total_rows:,} rows  in {n_batches} batches of {INSERT_BATCH:,}")
    t_start = time.time()
    inserted = 0

    for _ in range(n_batches):
        n = min(INSERT_BATCH, total_rows - inserted)

        # Timestamps as Python datetime objects (utcfromtimestamp is fast in map())
        ts_epochs   = base_epoch + np.random.randint(0, window_sec, n)
        occurred_at = list(map(datetime.utcfromtimestamp, ts_epochs.tolist()))

        punter_nums = np.random.randint(1, NUM_PUNTERS + 1, n)
        punter_ids  = np.char.add("p", punter_nums.astype(str))
        evt_types   = np.random.choice(cfg["event_types"], n)
        pay_idx     = np.random.randint(0, PAYLOAD_POOL, n)
        payloads    = payload_pool[pay_idx]

        col_data: list[Any] = [
            np.full(n, source_id),
            punter_ids,
        ]
        for dim in dims:
            col_data.append(np.random.choice(BET_SOURCES if dim == "bet_source" else [], n))
        col_data += [evt_types, occurred_at, payloads]

        client.insert(
            raw_t(source_id),
            [c.tolist() if hasattr(c, "tolist") else c for c in col_data],
            column_names=col_names,
            column_oriented=True,
        )

        inserted += n
        elapsed = time.time() - t_start
        rate    = inserted / elapsed
        eta     = (total_rows - inserted) / rate if rate > 0 else 0
        print(
            f"\r    {inserted:>12,}/{total_rows:,}  "
            f"{rate/1e3:5.0f}K rows/s  ETA {eta:4.0f}s  ",
            end="", flush=True,
        )

    print(f"\n    raw insert done  ({time.time()-t_start:.0f}s)")
    _materialize_rollups(client, source_id, cfg)


def _materialize_rollups(client, source_id: str, cfg: dict):
    dims       = cfg["extra_dims"]
    sel_extra  = (", ".join(f"dim_{d}" for d in dims) + ", ") if dims else ""
    amount_col = (
        f"sum({cfg['amount_expr']}) AS total_amount"
        if cfg["amount_expr"]
        else "0.0 AS total_amount"
    )

    for table, time_expr, time_col in [
        (daily_t(source_id),   "toDate(occurred_at)",          "day"),
        (monthly_t(source_id), "toStartOfMonth(occurred_at)",  "month"),
    ]:
        print(f"    materializing {table} ... ", end="", flush=True)
        t0 = time.time()
        client.command(
            f"INSERT INTO {table} "
            f"SELECT dim_punter_id, {sel_extra}event_type, "
            f"       {time_expr} AS {time_col}, "
            f"       count() AS event_count, {amount_col} "
            f"FROM {raw_t(source_id)} "
            f"GROUP BY dim_punter_id, {sel_extra}event_type, {time_col}",
            settings={
                "max_execution_time": 3600,
                "max_memory_usage": 10_000_000_000,
                "max_bytes_before_external_group_by": 5_000_000_000,
            },
        )
        print(f"{time.time()-t0:.0f}s")


# ── query catalogue ───────────────────────────────────────────────────────────
# Each entry:  label, tier, sql, needs_bs (bool, default False)

def build_catalogue() -> list[dict]:
    qs = []

    # ── punter_login ──────────────────────────────────────────────────────────
    s = "punter_login"
    qs += [
        dict(label=f"{s} | count  | 1d  | raw",
             tier="raw",
             sql=f"SELECT count() FROM {raw_t(s)} WHERE dim_punter_id=%(pid)s AND occurred_at >= now() - INTERVAL 1 DAY"),
        dict(label=f"{s} | count  | 7d  | mv_daily",
             tier="mv_daily",
             sql=f"SELECT sum(event_count) FROM {daily_t(s)} WHERE dim_punter_id=%(pid)s AND day >= today() - 7"),
        dict(label=f"{s} | count  | 30d | mv_daily",
             tier="mv_daily",
             sql=f"SELECT sum(event_count) FROM {daily_t(s)} WHERE dim_punter_id=%(pid)s AND day >= today() - 30"),
    ]

    # ── withdrawal ────────────────────────────────────────────────────────────
    s = "withdrawal"
    qs += [
        dict(label=f"{s} | count          | 1d  | raw",
             tier="raw",
             sql=f"SELECT count() FROM {raw_t(s)} WHERE dim_punter_id=%(pid)s AND occurred_at >= now() - INTERVAL 1 DAY"),
        dict(label=f"{s} | count          | 7d  | mv_daily",
             tier="mv_daily",
             sql=f"SELECT sum(event_count) FROM {daily_t(s)} WHERE dim_punter_id=%(pid)s AND day >= today() - 7"),
        dict(label=f"{s} | sum(amount)    | 7d  | raw",
             tier="raw",
             sql=f"SELECT sum(JSONExtractFloat(payload,'amount')) FROM {raw_t(s)} WHERE dim_punter_id=%(pid)s AND occurred_at >= now() - INTERVAL 7 DAY"),
        dict(label=f"{s} | sum(amount)    | 30d | raw",
             tier="raw",
             sql=f"SELECT sum(JSONExtractFloat(payload,'amount')) FROM {raw_t(s)} WHERE dim_punter_id=%(pid)s AND occurred_at >= now() - INTERVAL 30 DAY"),
        dict(label=f"{s} | lifetime_sum   | mv_monthly",
             tier="mv_monthly",
             sql=f"SELECT sum(total_amount) FROM {monthly_t(s)} WHERE dim_punter_id=%(pid)s"),
    ]

    # ── external_bet ──────────────────────────────────────────────────────────
    s = "external_bet"
    qs += [
        dict(label=f"{s} | count              | 1d  | raw",
             tier="raw",
             sql=f"SELECT count() FROM {raw_t(s)} WHERE dim_punter_id=%(pid)s AND occurred_at >= now() - INTERVAL 1 DAY"),
        dict(label=f"{s} | count              | 7d  | mv_daily",
             tier="mv_daily",
             sql=f"SELECT sum(event_count) FROM {daily_t(s)} WHERE dim_punter_id=%(pid)s AND day >= today() - 7"),
        dict(label=f"{s} | count              | 30d | mv_daily",
             tier="mv_daily",
             sql=f"SELECT sum(event_count) FROM {daily_t(s)} WHERE dim_punter_id=%(pid)s AND day >= today() - 30"),
        dict(label=f"{s} | count (compound)   | 7d  | mv_daily",
             tier="mv_daily",
             needs_bs=True,
             sql=f"SELECT sum(event_count) FROM {daily_t(s)} WHERE dim_punter_id=%(pid)s AND dim_bet_source=%(bs)s AND day >= today() - 7"),
        dict(label=f"{s} | sum(betAmount)     | 1d  | raw",
             tier="raw",
             sql=f"SELECT sum(JSONExtractFloat(JSONExtractRaw(payload,'betAmount'),'amount')) FROM {raw_t(s)} WHERE dim_punter_id=%(pid)s AND occurred_at >= now() - INTERVAL 1 DAY"),
        dict(label=f"{s} | sum(betAmount)     | 7d  | raw",
             tier="raw",
             sql=f"SELECT sum(JSONExtractFloat(JSONExtractRaw(payload,'betAmount'),'amount')) FROM {raw_t(s)} WHERE dim_punter_id=%(pid)s AND occurred_at >= now() - INTERVAL 7 DAY"),
        dict(label=f"{s} | lifetime_sum       | mv_monthly",
             tier="mv_monthly",
             sql=f"SELECT sum(total_amount) FROM {monthly_t(s)} WHERE dim_punter_id=%(pid)s"),
    ]

    return qs


# ── benchmark runner ──────────────────────────────────────────────────────────

def _run_query(client, sql: str, pid: str, bs: str | None) -> float:
    params: dict = {"pid": pid}
    if bs:
        params["bs"] = bs
    t0 = time.perf_counter()
    client.query(sql, parameters=params)
    return (time.perf_counter() - t0) * 1_000.0   # → ms


def _stats(latencies: list[float]) -> dict:
    s = sorted(latencies)
    n = len(s)
    return {
        "n":    n,
        "mean": round(statistics.mean(s), 1),
        "p50":  round(s[n // 2], 1),
        "p95":  round(s[int(n * 0.95)], 1),
        "p99":  round(s[int(n * 0.99)], 1),
        "max":  round(max(s), 1),
    }


def bench_query(get_client, q: dict, punter_pool: list[str]) -> tuple[dict, dict]:
    needs_bs = q.get("needs_bs", False)
    sql      = q["sql"]

    def pick() -> tuple[str, str | None]:
        return random.choice(punter_pool), (random.choice(BET_SOURCES) if needs_bs else None)

    c = get_client()

    # warm-up: 3 throwaway runs to prime CH's mark cache
    for _ in range(3):
        _run_query(c, sql, *pick())

    # single-threaded
    single_lats = [_run_query(c, sql, *pick()) for _ in range(BENCH_REPS)]

    # concurrent: 50 threads × CONCURRENT_REPS rounds
    conc_lats: list[float] = []
    for _ in range(CONCURRENT_REPS):
        with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as pool:
            futs = [
                pool.submit(_run_query, get_client(), sql, *pick())
                for _ in range(CONCURRENT_WORKERS)
            ]
            conc_lats.extend(f.result() for f in as_completed(futs))

    return _stats(single_lats), _stats(conc_lats)


# ── status ────────────────────────────────────────────────────────────────────

def print_status(client):
    print(f"\n  {'Table':<42} {'Rows':>14} {'Compressed':>12}")
    print("  " + "─" * 70)
    for sid in SOURCES:
        for tname in [raw_t(sid), daily_t(sid), monthly_t(sid)]:
            try:
                r = client.query(
                    "SELECT count(), formatReadableSize(sum(data_compressed_bytes)) "
                    f"FROM system.parts WHERE table='{tname}' AND active=1"
                )
                rows, size = r.result_rows[0]
                print(f"  {tname:<42} {rows:>14,} {size:>12}")
            except Exception:
                print(f"  {tname:<42} {'(missing)':>14}")


# ── rebuild rollups (no raw re-insert) ──────────────────────────────────────────

def truncate_rollups(client, source_id: str):
    for table in [daily_t(source_id), monthly_t(source_id)]:
        client.command(f"TRUNCATE TABLE IF EXISTS {table}")


# ── drop ──────────────────────────────────────────────────────────────────────

def drop_all(client, sources: list[str]):
    for sid in sources:
        for tname in [raw_t(sid), daily_t(sid), monthly_t(sid)]:
            client.command(f"DROP TABLE IF EXISTS {tname}")
            print(f"  dropped {tname}")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="EventMonitor Phase 1 — ClickHouse query latency benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--host",    default="localhost")
    ap.add_argument("--port",    type=int, default=8123)
    ap.add_argument("--seed",    action="store_true", help="seed tables with synthetic data")
    ap.add_argument("--bench",   action="store_true", help="run query latency benchmark")
    ap.add_argument("--status",  action="store_true", help="print row counts and compressed sizes")
    ap.add_argument("--drop",    action="store_true", help="drop all bm_* tables (use before re-seeding)")
    ap.add_argument("--materialize-only", action="store_true",
                    help="rebuild mv_daily/mv_monthly rollups from existing raw tables, "
                         "without re-inserting raw data (use to retry a failed materialization)")
    ap.add_argument("--sources", nargs="+", default=list(SOURCES.keys()),
                    choices=list(SOURCES.keys()),
                    metavar="SOURCE",
                    help="limit to specific sources (default: all three)")
    ap.add_argument("--output",  default="phase1_results.csv",
                    help="CSV file for benchmark results (default: phase1_results.csv)")
    args = ap.parse_args()

    if not any([args.seed, args.bench, args.status, args.drop, args.materialize_only]):
        ap.print_help()
        sys.exit(0)

    client = clickhouse_connect.get_client(host=args.host, port=args.port)
    print(f"Connected  →  {args.host}:{args.port}")

    # ── drop ─────────────────────────────────────────────────────────────────
    if args.drop:
        print("\n── DROP " + "─" * 60)
        drop_all(client, args.sources)

    # ── seed ──────────────────────────────────────────────────────────────────
    if args.seed:
        print("\n── SEED " + "─" * 60)
        print(f"\n  {'Source':<16} {'Rate':>8}  {'Rows':>14}  {'~Size':>10}")
        print("  " + "─" * 55)
        for sid in args.sources:
            cfg  = SOURCES[sid]
            rows = cfg["rate"] * 86_400 * RETENTION_DAYS
            gb   = rows * 80 / 1e9 / 8
            print(f"  {sid:<16} {cfg['rate']:>6}/s  {rows:>14,}  {gb:>8.1f} GB")
        print()

        _build_payload_pools()

        for sid in args.sources:
            provision_tables(client, sid, SOURCES[sid])
        for sid in args.sources:
            seed_source(client, sid, SOURCES[sid])

        print("\n  Seeding complete.")
        print_status(client)

    # ── materialize-only ─────────────────────────────────────────────────────
    if args.materialize_only:
        print("\n── MATERIALIZE-ONLY " + "─" * 48)
        for sid in args.sources:
            print(f"\n  {sid}: rebuilding rollups from existing raw data")
            provision_tables(client, sid, SOURCES[sid])
            truncate_rollups(client, sid)
            _materialize_rollups(client, sid, SOURCES[sid])

        print("\n  Rollups rebuilt.")
        print_status(client)

    # ── status ────────────────────────────────────────────────────────────────
    if args.status:
        print("\n── STATUS " + "─" * 58)
        print_status(client)

    # ── bench ─────────────────────────────────────────────────────────────────
    if args.bench:
        print("\n── BENCH " + "─" * 59)
        print(f"  Single-threaded: {BENCH_REPS} reps per query")
        print(f"  Concurrent:      {CONCURRENT_WORKERS} threads × {CONCURRENT_REPS} rounds = {CONCURRENT_WORKERS * CONCURRENT_REPS} samples per query\n")

        # Use a random subset of punters as the query population
        punter_pool = [f"p{i}" for i in random.sample(range(1, NUM_PUNTERS + 1), 2_000)]

        active_sources = set(args.sources)
        catalogue = [q for q in build_catalogue()
                     if any(q["label"].startswith(s) for s in active_sources)]

        def get_client():
            return clickhouse_connect.get_client(host=args.host, port=args.port)

        results: list[dict] = []

        for q in catalogue:
            print(f"  {q['label']}")
            st, ct = bench_query(get_client, q, punter_pool)
            slo_mark = "✓" if ct["p99"] < 200 else "✗ >200ms"
            print(f"    single  p50={st['p50']:>6.1f}ms  p95={st['p95']:>6.1f}ms  p99={st['p99']:>6.1f}ms  max={st['max']:>6.1f}ms")
            print(f"    conc    p50={ct['p50']:>6.1f}ms  p95={ct['p95']:>6.1f}ms  p99={ct['p99']:>6.1f}ms  max={ct['max']:>6.1f}ms  {slo_mark}")
            results.append({"label": q["label"], "tier": q["tier"], "mode": "single", **st})
            results.append({"label": q["label"], "tier": q["tier"], "mode": "conc",   **ct})

        # ── summary table ─────────────────────────────────────────────────────
        W = 66
        print("\n" + "─" * (W + 44))
        print(f"  {'Query':<{W}} {'Mode':<7} {'p50':>7} {'p95':>7} {'p99':>7} {'max':>7}  SLO(<200ms)")
        print("─" * (W + 44))
        for r in results:
            slo = "✓" if r["p99"] < 200 else "✗"
            print(f"  {r['label']:<{W}} {r['mode']:<7} {r['p50']:>6.1f}ms {r['p95']:>6.1f}ms "
                  f"{r['p99']:>6.1f}ms {r['max']:>6.1f}ms   {slo}")

        # ── CSV ───────────────────────────────────────────────────────────────
        fields = ["label", "tier", "mode", "n", "mean", "p50", "p95", "p99", "max"]
        with open(args.output, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=fields).writeheader()
            csv.DictWriter(f, fieldnames=fields).writerows(results)
        print(f"\n  Results written to {args.output}")


if __name__ == "__main__":
    main()
