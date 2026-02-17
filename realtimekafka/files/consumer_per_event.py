"""
Per-Event Consumer (BASELINE) — Process one event at a time.

This is the SLOW way: for each event, we:
  1. Deserialize JSON individually
  2. Run feature computation in Python
  3. Accumulate results one by one

This consumer exists to show WHY micro-batching is better.
Compare its throughput to consumer_microbatch.py.

Usage:
    python consumer_per_event.py
    python consumer_per_event.py --max-events 50000
"""

import time
import sys
import argparse
import signal
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError
import orjson

from config import KAFKA_BOOTSTRAP, TOPIC_RAW_EVENTS


# ─── Feature computation (per-event, Python dict style) ─────

class PerEventFeatureStore:
    """
    Naive per-event feature computation.
    For each event, update user-level aggregates one at a time.

    This is slow because:
    - Python dict lookups per event
    - No vectorization
    - Lots of object allocation
    - Each "write" is individual
    """
    def __init__(self):
        self.user_stats = defaultdict(lambda: {
            "event_count": 0,
            "click_count": 0,
            "purchase_count": 0,
            "total_spend": 0.0,
            "unique_items": set(),
            "last_event_ts": 0,
            "devices_seen": set(),
        })

    def process_event(self, event: dict):
        """Process a single event — the per-event overhead."""
        uid = event["user_id"]
        stats = self.user_stats[uid]

        stats["event_count"] += 1
        stats["last_event_ts"] = event["ts_ms"]
        stats["unique_items"].add(event["item_id"])
        stats["devices_seen"].add(event["device"])

        if event["event_type"] == "click":
            stats["click_count"] += 1
        elif event["event_type"] == "purchase":
            stats["purchase_count"] += 1
            stats["total_spend"] += event["amount"]

    @property
    def num_users(self):
        return len(self.user_stats)


def run_per_event_consumer(max_events: int = 0, timeout_sec: float = 30.0):
    """
    Consume events one at a time with per-event Python processing.
    """
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "per-event-consumer",
        "auto.offset.reset": "earliest",
        # Small fetch to simulate per-event behavior
        "fetch.min.bytes": 1,
        "max.poll.interval.ms": 300000,
    }

    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_RAW_EVENTS])

    store = PerEventFeatureStore()

    print(f"🐌 Per-Event Consumer started (baseline)")
    print(f"   Processing events one at a time...")
    print(f"   Topic: {TOPIC_RAW_EVENTS}")
    if max_events > 0:
        print(f"   Max events: {max_events:,}")
    print()

    processed = 0
    errors = 0
    t_start = time.perf_counter()
    t_last_report = t_start
    last_message_time = t_start

    # Graceful shutdown
    running = True
    def handle_signal(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, handle_signal)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message — check if we should stop
                if time.perf_counter() - last_message_time > timeout_sec:
                    print(f"\n  ⏱️  No messages for {timeout_sec}s — stopping")
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                errors += 1
                continue

            last_message_time = time.perf_counter()

            # ── PER-EVENT PROCESSING (the slow part) ──
            # 1. Deserialize one event
            event = orjson.loads(msg.value())

            # 2. Process one event
            store.process_event(event)

            processed += 1

            # Progress report
            now = time.perf_counter()
            if now - t_last_report >= 3.0:
                elapsed = now - t_start
                rate = processed / elapsed if elapsed > 0 else 0
                print(f"  🐌 {processed:>10,} events  "
                      f"({rate:,.0f} events/sec)  "
                      f"[{store.num_users:,} users tracked]")
                t_last_report = now

            if max_events > 0 and processed >= max_events:
                break

    finally:
        consumer.close()

    t_end = time.perf_counter()
    total_time = t_end - t_start
    final_rate = processed / total_time if total_time > 0 else 0

    print()
    print(f"🐌 Per-Event Consumer finished")
    print(f"   Processed:  {processed:,} events")
    print(f"   Errors:     {errors}")
    print(f"   Time:       {total_time:.2f}s")
    print(f"   Throughput: {final_rate:,.0f} events/sec")
    print(f"   Users:      {store.num_users:,}")

    return {
        "method": "per_event",
        "processed": processed,
        "time_sec": total_time,
        "rate": final_rate,
        "users_tracked": store.num_users,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Per-Event Consumer (Baseline)")
    parser.add_argument("--max-events", type=int, default=0,
                        help="Stop after N events (0=until timeout)")
    parser.add_argument("--timeout", type=float, default=15.0,
                        help="Stop after N seconds of no messages")
    args = parser.parse_args()

    run_per_event_consumer(args.max_events, args.timeout)
