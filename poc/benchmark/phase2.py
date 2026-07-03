#!/usr/bin/env python3
"""
EventMonitor — Phase 2 End-to-End Pipeline Latency Benchmark

Measures wall-clock time from the moment the first triggering event is committed
to the Kafka broker until the alert appears in GET /api/v1/alerts.

Pipeline under test:
  phase2.py → Kafka → KafkaConsumerManager → CHReadinessGate
             → RuleEvaluationOrchestrator → AlertManager → PostgreSQL
             ← GET /api/v1/alerts ← phase2.py

Timing:
  t_produce  = datetime.now() just before producer.send() of the first event in a scenario
  t_detect   = datetime.now() when polling loop first observes the alert
  latency    = t_detect − t_produce  (includes ≤ POLL_INTERVAL_S jitter)

Benchmark punters use IDs ≥ 9_000_000 to avoid colliding with normal traffic.

Usage:
  python3.11 phase2.py --bench
  python3.11 phase2.py --bench --trials 50 --concurrent 10 --conc-rounds 5
  python3.11 phase2.py --bench --background-rate 300   # production-rate background load
  python3.11 phase2.py --bench --scenarios large_withdrawal login_frequency
  python3.11 phase2.py --cleanup                       # resolve leftover BM2 alerts
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── constants ─────────────────────────────────────────────────────────────────

BM2_PUNTER_BASE  = 9_000_000   # trial punter IDs: 9_000_001, 9_000_002, …
POLL_INTERVAL_S  = 0.2         # alert polling cadence
TRIAL_TIMEOUT_S  = 120         # per-trial timeout

DEFAULT_TRIALS        = 30
DEFAULT_CONC_WORKERS  = 10
DEFAULT_CONC_ROUNDS   = 3

BET_SOURCES = ["WEB", "MOBILE", "API"]
BET_TYPES   = ["SINGLE", "ACCUMULATOR", "SYSTEM"]
COUNTRIES   = ["UK", "DE", "FR", "ES", "IT"]
CURRENCIES  = ["USD", "EUR", "GBP"]

# ── global punter counter (thread-safe) ───────────────────────────────────────

_punter_counter = 0
_punter_lock    = threading.Lock()

def next_punter_id() -> int:
    global _punter_counter
    with _punter_lock:
        _punter_counter += 1
        return BM2_PUNTER_BASE + _punter_counter

# ── helpers ───────────────────────────────────────────────────────────────────

def dim_key(dims: dict) -> str:
    """Sorted-key JSON — matches AlertManager.serializeDimensions."""
    return json.dumps(dict(sorted(dims.items())), separators=(',', ':'))

def iso_now() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000+00:00')

# ── event factories ───────────────────────────────────────────────────────────

def make_withdrawal(punter_id: int, amount: float) -> dict:
    return {
        "messageId": random.randint(1000, 99999),
        "transactionId": random.randint(1_000_000, 9_999_999),
        "operationType": "WITHDRAWAL",
        "punterInformation": {"punterId": punter_id, "country": random.choice(COUNTRIES)},
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "defaultCurrencyAmount": round(amount * 0.92, 2),
        "defaultCurrency": "EUR",
        "state": "COMPLETE",
        "timestamp": iso_now(),
        "constructMessageTimestamp": iso_now(),
    }

def make_login(punter_id: int) -> dict:
    return {
        "punterId": punter_id,
        "hostAddress": f"192.168.{random.randint(1,5)}.{random.randint(1,254)}",
        "countryCode": random.choice(COUNTRIES),
        "authMethod": random.choice(["PASSWORD", "BIOMETRIC", "SMS_OTP"]),
        "openTime": int(time.time() * 1000),
        "eventType": "LOGIN",
    }

def make_bet(punter_id: int, amount: float, bet_source: str) -> dict:
    return {
        "id": random.randint(10_000_000, 99_999_999),
        "punterId": punter_id,
        "betAmount": {"amount": amount, "currency": "USD"},
        "betAmountDefCur": {"amount": round(amount * 0.92, 2), "currency": "EUR"},
        "betType": random.choice(BET_TYPES),
        "betState": "OPEN",
        "betSource": bet_source,
        "betTime": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "winAmount": {"amount": 0.0, "currency": "USD"},
    }

# ── scenario definitions ──────────────────────────────────────────────────────

class Scenario:
    """
    events_fn(punter_id) → list of all events to produce in order.
    The alert should fire after the last event.
    dims_fn(punter_id)   → dict used to compute the expected entity_dimension_value.
    """
    def __init__(self, key, rule_name, topic, events_fn, dims_fn):
        self.key       = key
        self.rule_name = rule_name
        self.topic     = topic
        self.events_fn = events_fn
        self.dims_fn   = dims_fn
        self.rule_id: Optional[str] = None  # resolved at startup from GET /api/v1/rules

# Rule: "Large Withdrawal Alert"  →  payload.amount > 500  (1 event, no aggregation)
# Rule: "Suspicious Login Frequency"  →  agg_count("punter_login","punter_id",1) > 3  (4 events)
# Rule: "High Daily Betting Volume"   →  agg_sum("external_bet","punter_id","betAmount.amount",1) > 500  (3 × $200)
# Rule: "Concentrated Source Betting" →  agg_count("external_bet","punter_id|bet_source",7) > 20  (21 bets)

SCENARIOS = [
    Scenario(
        key="large_withdrawal",
        rule_name="Large Withdrawal Alert",
        topic="withdrawal",
        events_fn=lambda p: [make_withdrawal(p, 1000.0)],
        dims_fn=lambda p: {"punter_id": str(p)},
    ),
    Scenario(
        key="login_frequency",
        rule_name="Suspicious Login Frequency",
        topic="punter-auth-success-login",
        events_fn=lambda p: [make_login(p)] * 4,
        dims_fn=lambda p: {"punter_id": str(p)},
    ),
    Scenario(
        key="high_bet_volume",
        rule_name="High Daily Betting Volume",
        topic="ebs_bets",
        events_fn=lambda p: [make_bet(p, 200.0, "WEB")] * 3,
        # external_bet has 2 dims; AlertManager serialises all dims
        dims_fn=lambda p: {"bet_source": "WEB", "punter_id": str(p)},
    ),
    Scenario(
        key="concentrated_source",
        rule_name="Concentrated Source Betting",
        topic="ebs_bets",
        events_fn=lambda p: [make_bet(p, 10.0, "WEB")] * 21,
        dims_fn=lambda p: {"bet_source": "WEB", "punter_id": str(p)},
    ),
]

# ── trial ─────────────────────────────────────────────────────────────────────

def run_trial(
    scenario: Scenario,
    producer: KafkaProducer,
    backend_url: str,
) -> Optional[float]:
    """Produce events, poll for alert. Returns latency in ms or None (timeout)."""
    punter_id    = next_punter_id()
    expected_dim = dim_key(scenario.dims_fn(punter_id))
    events       = scenario.events_fn(punter_id)

    # Record produce time before the first send, flush to ensure broker receipt
    t_produce = datetime.now(timezone.utc)
    for evt in events:
        producer.send(scenario.topic, value=json.dumps(evt, default=str).encode())
    producer.flush()

    # Poll until alert appears or timeout
    deadline = time.monotonic() + TRIAL_TIMEOUT_S
    while time.monotonic() < deadline:
        try:
            resp = requests.get(
                f"{backend_url}/api/v1/alerts",
                params={"status": "OPEN"},
                timeout=5,
            )
            resp.raise_for_status()
            for a in resp.json():
                if (a.get("ruleId") == scenario.rule_id and
                        a.get("entityDimensionValue") == expected_dim):
                    t_detect = datetime.now(timezone.utc)
                    latency  = (t_detect - t_produce).total_seconds() * 1000
                    _resolve(backend_url, a["id"])
                    return latency
        except Exception:
            pass
        time.sleep(POLL_INTERVAL_S)

    return None   # timed out

def _resolve(backend_url: str, alert_id: str):
    try:
        requests.post(f"{backend_url}/api/v1/alerts/{alert_id}/resolve", timeout=5)
    except Exception:
        pass

# ── background load ───────────────────────────────────────────────────────────

def _background_load(producer: KafkaProducer, rate: int, stop: threading.Event):
    interval = 1.0 / rate
    topics = [
        ("punter-auth-success-login", lambda p: make_login(p)),
        ("withdrawal",                lambda p: make_withdrawal(p, random.uniform(10, 400))),
        ("ebs_bets",                  lambda p: make_bet(p, random.uniform(5, 50),
                                                         random.choice(BET_SOURCES))),
    ]
    i = 0
    while not stop.is_set():
        topic, fn = topics[i % 3]
        p = random.randint(1, 1000)
        try:
            producer.send(topic, value=json.dumps(fn(p), default=str).encode())
        except Exception:
            pass
        i += 1
        stop.wait(interval)

# ── stats ─────────────────────────────────────────────────────────────────────

def compute_stats(lats: list[float], timeouts: int) -> dict:
    s = sorted(lats)
    n = len(s)
    return {
        "n":       n,
        "mean":    round(statistics.mean(s)),
        "p50":     round(s[n // 2]),
        "p95":     round(s[int(n * 0.95)]),
        "p99":     round(s[int(n * 0.99)]),
        "max":     round(max(s)),
        "timeouts": timeouts,
    }

# ── cleanup ───────────────────────────────────────────────────────────────────

def cleanup(backend_url: str):
    print("  Resolving BM2 open/acknowledged alerts... ", end="", flush=True)
    resolved = 0
    for status in ("OPEN", "ACKNOWLEDGED"):
        try:
            for a in requests.get(f"{backend_url}/api/v1/alerts",
                                   params={"status": status}, timeout=10).json():
                dim = a.get("entityDimensionValue", "")
                # BM2 punter IDs are always >= 9_000_001
                try:
                    # The dim value is JSON; check if any numeric value >= BM2_PUNTER_BASE
                    parsed = json.loads(dim)
                    if any(int(v) >= BM2_PUNTER_BASE for v in parsed.values()
                           if str(v).isdigit()):
                        _resolve(backend_url, a["id"])
                        resolved += 1
                except Exception:
                    pass
        except Exception:
            pass
    print(f"{resolved} resolved.")

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="EventMonitor Phase 2 — end-to-end pipeline latency benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--bench",   action="store_true", help="run the benchmark")
    ap.add_argument("--cleanup", action="store_true", help="resolve leftover BM2 alerts then exit")
    ap.add_argument("--backend", default="http://localhost:8080")
    ap.add_argument("--kafka",   default="localhost:9092")
    ap.add_argument("--trials",  type=int, default=DEFAULT_TRIALS,
                    help=f"sequential trials per scenario (default {DEFAULT_TRIALS})")
    ap.add_argument("--concurrent", type=int, default=DEFAULT_CONC_WORKERS,
                    help=f"concurrent workers (default {DEFAULT_CONC_WORKERS})")
    ap.add_argument("--conc-rounds", type=int, default=DEFAULT_CONC_ROUNDS,
                    help=f"concurrent rounds (default {DEFAULT_CONC_ROUNDS})")
    ap.add_argument("--background-rate", type=int, default=0,
                    help="background events/sec to simulate pipeline load (default 0)")
    ap.add_argument("--scenarios", nargs="+",
                    default=[s.key for s in SCENARIOS],
                    choices=[s.key for s in SCENARIOS],
                    metavar="SCENARIO")
    ap.add_argument("--output", default="phase2_results.csv")
    args = ap.parse_args()

    if not any([args.bench, args.cleanup]):
        ap.print_help()
        sys.exit(0)

    # ── resolve rule IDs from backend ─────────────────────────────────────────
    print(f"Backend  →  {args.backend}")
    try:
        rules = {r["name"]: r for r in
                 requests.get(f"{args.backend}/api/v1/rules", timeout=10).json()}
    except Exception as e:
        sys.exit(f"Cannot reach backend: {e}")

    active = []
    for s in SCENARIOS:
        if s.key not in args.scenarios:
            continue
        if s.rule_name in rules:
            s.rule_id = rules[s.rule_name]["id"]
            print(f"  ✓  {s.rule_name}  ({s.rule_id})")
            active.append(s)
        else:
            print(f"  ✗  {s.rule_name}  — not found in backend, skipping")

    if args.cleanup:
        cleanup(args.backend)
        if not args.bench:
            return

    if not args.bench:
        return

    if not active:
        sys.exit("No scenarios to run.")

    # ── connect Kafka ─────────────────────────────────────────────────────────
    print(f"\nKafka  →  {args.kafka}")
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.kafka,
                value_serializer=lambda v: v if isinstance(v, bytes) else v.encode(),
            )
            print("  connected.\n")
            break
        except NoBrokersAvailable:
            print(f"  not ready ({attempt+1}/10), retrying in 3s...")
            time.sleep(3)
    else:
        sys.exit("Could not connect to Kafka.")

    # ── background load ───────────────────────────────────────────────────────
    stop_bg = threading.Event()
    if args.background_rate > 0:
        threading.Thread(
            target=_background_load,
            args=(producer, args.background_rate, stop_bg),
            daemon=True,
            name="bg",
        ).start()
        print(f"Background load: {args.background_rate} events/s  (running)\n")

    results: list[dict] = []

    for scenario in active:
        n_events = len(scenario.events_fn(BM2_PUNTER_BASE))
        print(f"── {scenario.rule_name}  [{n_events} event(s) per trial]")

        # ── sequential ────────────────────────────────────────────────────────
        print(f"  Sequential  ({args.trials} trials):")
        seq_lats, seq_to = [], 0
        for i in range(args.trials):
            lat = run_trial(scenario, producer, args.backend)
            if lat is None:
                seq_to += 1
                print(f"    [{i+1:>3}]  TIMEOUT")
            else:
                seq_lats.append(lat)
                print(f"    [{i+1:>3}]  {lat:,.0f} ms")

        if seq_lats:
            st = compute_stats(seq_lats, seq_to)
            print(f"  → mean={st['mean']}ms  p50={st['p50']}ms  "
                  f"p95={st['p95']}ms  p99={st['p99']}ms  "
                  f"max={st['max']}ms  timeouts={seq_to}\n")
            results.append({"scenario": scenario.key, "rule": scenario.rule_name,
                            "mode": "sequential", **st})
        else:
            print(f"  → all {seq_to} trials timed out\n")

        # ── concurrent ────────────────────────────────────────────────────────
        total_concurrent = args.concurrent * args.conc_rounds
        print(f"  Concurrent  ({args.concurrent} workers × {args.conc_rounds} rounds"
              f" = {total_concurrent} trials):")
        conc_lats, conc_to = [], 0
        for r in range(args.conc_rounds):
            with ThreadPoolExecutor(max_workers=args.concurrent) as pool:
                futs = [pool.submit(run_trial, scenario, producer, args.backend)
                        for _ in range(args.concurrent)]
                for f in as_completed(futs):
                    lat = f.result()
                    if lat is None:
                        conc_to += 1
                    else:
                        conc_lats.append(lat)
            print(f"    round {r+1}/{args.conc_rounds}: "
                  f"{len(conc_lats)} done, {conc_to} timeouts")

        if conc_lats:
            ct = compute_stats(conc_lats, conc_to)
            print(f"  → mean={ct['mean']}ms  p50={ct['p50']}ms  "
                  f"p95={ct['p95']}ms  p99={ct['p99']}ms  "
                  f"max={ct['max']}ms  timeouts={conc_to}\n")
            results.append({"scenario": scenario.key, "rule": scenario.rule_name,
                            "mode": "concurrent", **ct})

    # ── stop background ───────────────────────────────────────────────────────
    stop_bg.set()
    producer.close()

    # ── summary ───────────────────────────────────────────────────────────────
    if results:
        W = 37
        print("─" * (W + 56))
        print(f"  {'Rule':<{W}} {'Mode':<12} {'p50':>7} {'p95':>7} {'p99':>7} {'max':>7}  Timeouts")
        print("─" * (W + 56))
        for r in results:
            print(f"  {r['rule']:<{W}} {r['mode']:<12} "
                  f"{r['p50']:>6}ms {r['p95']:>6}ms "
                  f"{r['p99']:>6}ms {r['max']:>6}ms  {r['timeouts']}")

        fields = ["scenario", "rule", "mode", "n", "mean", "p50", "p95", "p99", "max", "timeouts"]
        with open(args.output, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(results)
        print(f"\n  Results written to {args.output}")


if __name__ == "__main__":
    main()
