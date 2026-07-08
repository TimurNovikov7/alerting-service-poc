#!/usr/bin/env python3
"""
EventMonitor — Phase 4 Real-Table Backfill

Bulk-inserts synthetic historical events DIRECTLY into the REAL, live
events_<source> tables — the ones created and used by the running backend's
ClickHouseProvisioner — so that aggregation queries (phase2/phase3 traffic,
manual testing) scan realistically-sized tables instead of empty or
near-empty ones. Bypasses Kafka and the backend entirely: no rule evaluation,
no alerts, no Postgres writes. This only touches ClickHouse.

mv_daily_<source> and mv_monthly_<source> are LIVE materialized views the
backend already attached to events_<source> — inserting here updates them
automatically on every batch, no separate materialization step needed
(unlike phase1.py, which seeds isolated bm_* tables with no live MVs
attached, and has to backfill the rollups itself via INSERT...SELECT).

This script never issues CREATE TABLE — the tables must already exist
(the backend creates them at startup). It only inserts into them.

WARNING: these are the SAME tables the live backend queries for real rule
evaluation. Backfilling data for a punter ID raises that punter's aggregate
baseline going forward — live traffic (background load, phase2/phase3
scenario runs) touching the same punter IDs may cross rule thresholds more
easily than before. This is a shared, live system, not an isolated sandbox
like phase1.py's bm_* tables. Punter IDs are drawn from the same 1..N pool
phase2.py/phase3.py already use, so backfilled history and live traffic
refer to the same "punters" by design.

Usage
  python phase4.py --seed                                # backfill all 3 sources
  python phase4.py --seed --sources external_bet         # one source only
  python phase4.py --seed --rate 5 --retention-days 14    # lighter volume
  python phase4.py --status                               # row counts for real tables
  python phase4.py --host 192.168.1.10 --port 8123
"""

from __future__ import annotations

import argparse
import math
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import clickhouse_connect
import numpy as np

# ── parameters ───────────────────────────────────────────────────────────────

PUNTER_POOL    = 100_000    # same pool phase2.py/phase3.py draw from
INSERT_BATCH   = 500_000    # rows per INSERT call
PAYLOAD_POOL   = 50_000     # pre-built payload strings; sampled via numpy indexing

BET_SOURCES = ["WEB", "MOBILE", "API"]
BET_TYPES   = ["SINGLE", "ACCUMULATOR", "SYSTEM"]
COUNTRIES   = ["UK", "DE", "FR", "ES", "IT", "PL", "NL", "SE", "NO", "AU"]
CURRENCIES  = ["USD", "EUR", "GBP"]
AUTH_METHODS = ["PASSWORD", "BIOMETRIC", "SMS_OTP"]

# Populated once at startup in _build_payload_pools()
_POOLS: dict[str, np.ndarray] = {}

# ── source definitions ────────────────────────────────────────────────────────
# rate         — default events/sec to backfill (override with --rate)
# extra_dims   — dimension columns beyond dim_punter_id (matches real schema)
# event_types  — values for the event_type column (matches rule triggerEventType)
#
# Defaults total 300 events/s, matching the --background-rate 300 profile used
# throughout phase2.py/phase3.py testing: 250 bet, 40 withdrawal, 10 login.

SOURCES: dict[str, dict] = {
    "punter_login": {
        "rate":        10,
        "extra_dims":  [],
        "event_types": ["LOGIN"],
    },
    "withdrawal": {
        "rate":        40,
        "extra_dims":  [],
        "event_types": ["withdrawal"],
    },
    "external_bet": {
        "rate":        250,
        "extra_dims":  ["bet_source"],
        "event_types": BET_TYPES,
    },
}


def _build_payload_pools():
    """Real payload shapes, matching make_login/make_withdrawal/make_bet in
    phase2.py exactly, so JSONExtractFloat dot-paths used by AggregationQueryService
    resolve correctly. The punterId embedded inside the JSON is sampled independently
    of the row's dim_punter_id column — no query filters on that embedded value, only
    on dim_punter_id itself, so decoupling it keeps pool construction fast at scale.
    """
    rng = np.random
    amounts = np.round(rng.uniform(5.0, 5_000.0, PAYLOAD_POOL), 2)
    fake_ids = rng.randint(1, PUNTER_POOL + 1, PAYLOAD_POOL)

    # punter_login — real shape from make_login()
    ips       = [f"192.168.{rng.randint(1,6)}.{rng.randint(1,255)}" for _ in range(PAYLOAD_POOL)]
    countries = rng.choice(COUNTRIES, PAYLOAD_POOL)
    methods   = rng.choice(AUTH_METHODS, PAYLOAD_POOL)
    open_ts   = rng.randint(1_700_000_000_000, 1_900_000_000_000, PAYLOAD_POOL)
    _POOLS["punter_login"] = np.array([
        f'{{"punterId":{pid},"hostAddress":"{ip}","countryCode":"{c}",'
        f'"authMethod":"{m}","openTime":{ot},"eventType":"LOGIN"}}'
        for pid, ip, c, m, ot in zip(fake_ids, ips, countries, methods, open_ts)
    ])

    # withdrawal — real shape from make_withdrawal()
    currencies = rng.choice(CURRENCIES, PAYLOAD_POOL)
    wcountries = rng.choice(COUNTRIES, PAYLOAD_POOL)
    def_amounts = np.round(amounts * 0.92, 2)
    _POOLS["withdrawal"] = np.array([
        f'{{"messageId":{rng.randint(1000,99999)},"transactionId":{rng.randint(1000000,9999999)},'
        f'"operationType":"WITHDRAWAL","punterInformation":{{"punterId":{pid},"country":"{c}"}},'
        f'"amount":{a},"currency":"{cur}","defaultCurrencyAmount":{da},"defaultCurrency":"EUR",'
        f'"state":"COMPLETE"}}'
        for pid, c, a, cur, da in zip(fake_ids, wcountries, amounts, currencies, def_amounts)
    ])

    # external_bet — real shape from make_bet()
    bet_types   = rng.choice(BET_TYPES, PAYLOAD_POOL)
    bet_sources = rng.choice(BET_SOURCES, PAYLOAD_POOL)
    bet_def_amounts = np.round(amounts * 0.92, 2)
    _POOLS["external_bet"] = np.array([
        f'{{"id":{rng.randint(10000000,99999999)},"punterId":{pid},'
        f'"betAmount":{{"amount":{a},"currency":"USD"}},'
        f'"betAmountDefCur":{{"amount":{da},"currency":"EUR"}},'
        f'"betType":"{bt}","betState":"OPEN","betSource":"{bs}",'
        f'"winAmount":{{"amount":0.0,"currency":"USD"}}}}'
        for pid, a, da, bt, bs in zip(fake_ids, amounts, bet_def_amounts, bet_types, bet_sources)
    ])


# ── table names (real, no bm_ prefix — must already exist) ───────────────────

def raw_t(s: str)     -> str: return f"events_{s}"
def daily_t(s: str)   -> str: return f"mv_daily_{s}"
def monthly_t(s: str) -> str: return f"mv_monthly_{s}"


def check_tables_exist(client, source_id: str) -> bool:
    for t in [raw_t(source_id), daily_t(source_id), monthly_t(source_id)]:
        try:
            result = client.query(f"EXISTS TABLE {t}")
            if not result.result_rows[0][0]:
                print(f"  ✗  {t} does not exist — is the backend running and has it "
                      f"provisioned this source? Skipping {source_id}.")
                return False
        except Exception as e:
            print(f"  ✗  Could not check {t}: {e}. Skipping {source_id}.")
            return False
    return True


# ── seeding ───────────────────────────────────────────────────────────────────

def seed_source(client, source_id: str, cfg: dict, rate: int, retention_days: int):
    total_rows  = rate * 86_400 * retention_days
    n_batches   = math.ceil(total_rows / INSERT_BATCH)
    window_sec  = retention_days * 86_400
    base_epoch  = int((datetime.now(timezone.utc) - timedelta(days=retention_days)).timestamp())
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

        ts_epochs   = base_epoch + np.random.randint(0, window_sec, n)
        occurred_at = list(map(datetime.utcfromtimestamp, ts_epochs.tolist()))

        # Plain numeric punter IDs as strings — matches real dim_punter_id values
        # written by the live pipeline (no "p" prefix, unlike phase1.py's bm_* tables).
        punter_nums = np.random.randint(1, PUNTER_POOL + 1, n)
        punter_ids  = punter_nums.astype(str)
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
        rate_actual = inserted / elapsed
        eta = (total_rows - inserted) / rate_actual if rate_actual > 0 else 0
        print(
            f"\r    {inserted:>12,}/{total_rows:,}  "
            f"{rate_actual/1e3:5.0f}K rows/s  ETA {eta:4.0f}s  ",
            end="", flush=True,
        )

    print(f"\n    done ({time.time()-t_start:.0f}s) — mv_daily/mv_monthly update "
          f"automatically via the live attached materialized views")


# ── status ────────────────────────────────────────────────────────────────────

def print_status(client):
    print(f"\n  {'Table':<42} {'Rows':>14} {'Compressed':>12}")
    print("  " + "─" * 70)
    for sid in SOURCES:
        for tname in [raw_t(sid), daily_t(sid), monthly_t(sid)]:
            try:
                rows = client.query(f"SELECT count() FROM {tname}").result_rows[0][0]
            except Exception:
                print(f"  {tname:<42} {'(missing)':>14}")
                continue
            # mv_daily_*/mv_monthly_* are materialized views with their own ENGINE
            # (not "TO target_table"), so ClickHouse stores their data under a
            # hidden .inner_id.<uuid> table — system.parts doesn't know the
            # view's friendly name, so compressed size isn't available this way
            # for those two; only the raw events_* table appears there directly.
            try:
                size_rows = client.query(
                    "SELECT formatReadableSize(sum(data_compressed_bytes)) "
                    f"FROM system.parts WHERE table='{tname}' AND active=1"
                ).result_rows
                size = size_rows[0][0] if size_rows and size_rows[0][0] else "n/a"
            except Exception:
                size = "n/a"
            print(f"  {tname:<42} {rows:>14,} {size:>12}")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="EventMonitor Phase 4 — backfill the real events_* tables "
                     "for realistic query scan volumes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--host",   default="localhost")
    ap.add_argument("--port",   type=int, default=8123)
    ap.add_argument("--seed",   action="store_true", help="backfill events_* with synthetic historical data")
    ap.add_argument("--status", action="store_true", help="print row counts and compressed sizes")
    ap.add_argument("--sources", nargs="+", default=list(SOURCES.keys()),
                    choices=list(SOURCES.keys()),
                    metavar="SOURCE",
                    help="limit to specific sources (default: all three)")
    ap.add_argument("--rate", type=int, default=None,
                    help="override events/sec for ALL selected sources "
                         "(default: per-source defaults — punter_login=10, withdrawal=40, external_bet=250)")
    ap.add_argument("--retention-days", type=int, default=30,
                    help="days of history to backfill (default 30)")
    args = ap.parse_args()

    if not any([args.seed, args.status]):
        ap.print_help()
        sys.exit(0)

    client = clickhouse_connect.get_client(host=args.host, port=args.port)
    print(f"Connected  →  {args.host}:{args.port}")

    if args.seed:
        print("\n── SEED (real events_* tables) " + "─" * 40)
        print("  WARNING: these are the live tables the backend queries for real rule\n"
              "  evaluation. Backfilled punter IDs will have a higher aggregate baseline\n"
              "  going forward — live traffic touching the same IDs may cross rule\n"
              "  thresholds more easily than before.\n")

        print(f"  {'Source':<16} {'Rate':>8}  {'Rows':>14}")
        print("  " + "─" * 45)
        for sid in args.sources:
            rate = args.rate if args.rate is not None else SOURCES[sid]["rate"]
            rows = rate * 86_400 * args.retention_days
            print(f"  {sid:<16} {rate:>6}/s  {rows:>14,}")
        print()

        _build_payload_pools()

        for sid in args.sources:
            if not check_tables_exist(client, sid):
                continue
            rate = args.rate if args.rate is not None else SOURCES[sid]["rate"]
            seed_source(client, sid, SOURCES[sid], rate, args.retention_days)

        print("\n  Seeding complete.")
        print_status(client)

    if args.status:
        print("\n── STATUS " + "─" * 58)
        print_status(client)


if __name__ == "__main__":
    main()
