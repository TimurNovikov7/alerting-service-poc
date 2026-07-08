#!/usr/bin/env python3
"""
EventMonitor — Phase 3 Background Event Generator

Pure load generator: produces synthetic events at a specified total rate for
specified event types only. No rule-evaluation/alert-detection measurement —
use this to isolate a single topic's own consumer/evaluation throughput
(e.g. "does ebs_bets alone, at 300/s, keep pace, independent of any other
topic's traffic?") without phase2.py's scenario trials or alert polling as
confounding factors.

Usage:
  python3 phase3.py --rate 300 --event-type ebs_bets withdrawal punter_login
  python3 phase3.py --rate 300 --event-type ebs_bets              # isolate one topic
  python3 phase3.py --rate 100 --event-type punter_login --duration 60
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import threading
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from phase2 import make_login, make_withdrawal, make_bet, BET_SOURCES

PUNTER_POOL_DEFAULT = 100_000

# name → (topic, event factory)
EVENT_TYPES = {
    "punter_login": ("punter-auth-success-login", lambda p: make_login(p)),
    "withdrawal":   ("withdrawal",                lambda p: make_withdrawal(p, random.uniform(10, 400))),
    "ebs_bets":     ("ebs_bets",                   lambda p: make_bet(p, random.uniform(5, 50),
                                                                      random.choice(BET_SOURCES))),
}


def _generator_worker(topic: str, event_fn, rate: float, punter_pool: int,
                       stop: threading.Event, producer: KafkaProducer,
                       sent_counts: list[int], idx: int):
    interval = 1.0 / rate
    sent = 0
    next_send_at = time.monotonic()
    while not stop.is_set():
        p = random.randint(1, punter_pool)
        try:
            producer.send(topic, value=json.dumps(event_fn(p), default=str).encode(),
                           key=str(p).encode())
            sent += 1
        except Exception:
            pass
        # Fixed schedule, not "sleep interval after each send" — see phase2.py's
        # _background_load for why: per-iteration overhead otherwise stacks on
        # top of interval and the achieved rate falls short of target.
        next_send_at += interval
        remaining = next_send_at - time.monotonic()
        if remaining > 0:
            stop.wait(remaining)
    sent_counts[idx] = sent


def main():
    ap = argparse.ArgumentParser(
        description="EventMonitor Phase 3 — pure background event generator "
                     "(no alert detection or latency measurement)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--kafka", default="localhost:29092")
    ap.add_argument("--rate", type=float, default=100.0,
                    help="total target events/s, split evenly across selected "
                         "--event-type (default 100)")
    ap.add_argument("--event-type", nargs="+", dest="event_types",
                    default=list(EVENT_TYPES.keys()),
                    choices=list(EVENT_TYPES.keys()),
                    metavar="TYPE",
                    help=f"event types to generate (default: all — {', '.join(EVENT_TYPES.keys())})")
    ap.add_argument("--punter-pool", type=int, default=PUNTER_POOL_DEFAULT,
                    help=f"random punter ID range 1..N (default {PUNTER_POOL_DEFAULT})")
    ap.add_argument("--duration", type=float, default=None,
                    help="run for this many seconds, then stop (default: run until Ctrl+C)")
    args = ap.parse_args()

    print(f"Kafka → {args.kafka}")
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.kafka,
                value_serializer=lambda v: v if isinstance(v, bytes) else v.encode(),
            )
            print("  connected.\n")
            break
        except NoBrokersAvailable:
            print(f"  not ready ({attempt + 1}/10), retrying in 3s...")
            time.sleep(3)
    else:
        sys.exit("Could not connect to Kafka.")

    n_types = len(args.event_types)
    per_type_rate = args.rate / n_types
    print(f"Generating {args.rate:.0f} events/s total across {n_types} type(s) "
          f"({per_type_rate:.1f}/s each): {', '.join(args.event_types)}")
    print(f"Running for {args.duration:.0f}s...\n" if args.duration
          else "Running until Ctrl+C...\n")

    stop = threading.Event()
    sent_counts = [0] * n_types
    threads = []
    t_start = time.monotonic()
    for idx, name in enumerate(args.event_types):
        topic, event_fn = EVENT_TYPES[name]
        t = threading.Thread(
            target=_generator_worker,
            args=(topic, event_fn, per_type_rate, args.punter_pool, stop, producer,
                  sent_counts, idx),
            daemon=True, name=f"gen-{name}",
        )
        t.start()
        threads.append(t)

    try:
        if args.duration:
            stop.wait(args.duration)
        else:
            while True:
                time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n  Stopping...")
    stop.set()

    for t in threads:
        t.join(timeout=2)
    elapsed = time.monotonic() - t_start
    producer.close()

    print(f"\n── Summary ({elapsed:.1f}s) ─────────────────────────────")
    total = 0
    for idx, name in enumerate(args.event_types):
        n = sent_counts[idx]
        total += n
        rate = n / elapsed if elapsed > 0 else 0
        print(f"  {name:<14} {n:>8} events  ≈ {rate:>6.1f}/s  (target {per_type_rate:.1f}/s)")
    total_rate = total / elapsed if elapsed > 0 else 0
    print(f"  {'TOTAL':<14} {total:>8} events  ≈ {total_rate:>6.1f}/s  (target {args.rate:.0f}/s)")


if __name__ == "__main__":
    main()
