"""
Event Producer — Simulates high-volume user activity stream.

This is the "frontend / services publish events" part of the pipeline.
In production, services like the app backend, search service, checkout
service, etc. each publish events to Kafka topics.

Usage:
    python producer.py                    # default: 100k events
    python producer.py --count 1000000    # 1M events
    python producer.py --rate 50000       # target 50k events/sec
"""

import time
import sys
import argparse
from confluent_kafka import Producer, KafkaError
from config import (
    KAFKA_BOOTSTRAP, TOPIC_RAW_EVENTS, generate_event
)


def delivery_callback(err, msg):
    """Called once per message to confirm delivery (or report failure)."""
    if err is not None:
        print(f"  ❌ Delivery failed: {err}")


def run_producer(total_events: int, target_rate: int = 0, verbose: bool = False):
    """
    Produce events to Kafka as fast as possible (or at target_rate).

    Args:
        total_events: how many events to send
        target_rate:  events/sec limit (0 = unlimited)
        verbose:      print every 10k events
    """
    # Producer config — tuned for throughput
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "event-producer",

        # ── Throughput tuning ──
        # batch.size: how many bytes to batch before sending (default 16KB)
        "batch.size": 65536,          # 64KB batches

        # linger.ms: wait up to N ms to fill the batch (default 0)
        # Higher = more batching = higher throughput, slightly higher latency
        "linger.ms": 10,

        # compression: reduces network I/O at cost of CPU
        "compression.type": "lz4",

        # buffer.memory: total memory for unsent messages
        "queue.buffering.max.kbytes": 67108864,    # 64MB

        # Queue size for async delivery
        "queue.buffering.max.messages": 500000,
    }

    producer = Producer(conf)
    topic = TOPIC_RAW_EVENTS

    print(f"🚀 Producing {total_events:,} events to '{topic}'...")
    if target_rate > 0:
        print(f"   Rate limit: {target_rate:,} events/sec")
    print(f"   Config: batch.size=64KB, linger.ms=10, compression=lz4")
    print()

    sent = 0
    errors = 0
    t_start = time.perf_counter()
    t_last_report = t_start

    for i in range(total_events):
        event = generate_event()

        # Partition by user_id for ordering guarantees per user
        # (same user's events always go to same partition)
        try:
            producer.produce(
                topic=topic,
                key=event.user_id.encode("utf-8"),
                value=event.to_json_bytes(),
                callback=delivery_callback if verbose else None,
            )
            sent += 1
        except BufferError:
            # Internal queue full — flush and retry
            producer.flush(timeout=5)
            producer.produce(
                topic=topic,
                key=event.user_id.encode("utf-8"),
                value=event.to_json_bytes(),
            )
            sent += 1

        # Periodic flush to trigger delivery callbacks
        if sent % 10000 == 0:
            producer.poll(0)  # trigger callbacks without blocking

            now = time.perf_counter()
            elapsed = now - t_start
            rate = sent / elapsed if elapsed > 0 else 0

            if now - t_last_report >= 2.0:
                print(f"  📤 {sent:>10,} / {total_events:,}  "
                      f"({rate:,.0f} events/sec)  "
                      f"[{elapsed:.1f}s elapsed]")
                t_last_report = now

        # Rate limiting
        if target_rate > 0 and sent % 1000 == 0:
            elapsed = time.perf_counter() - t_start
            expected = sent / target_rate
            if elapsed < expected:
                time.sleep(expected - elapsed)

    # Final flush — wait for all messages to be delivered
    remaining = producer.flush(timeout=30)
    t_end = time.perf_counter()

    total_time = t_end - t_start
    final_rate = sent / total_time

    print()
    print(f"✅ Producer finished")
    print(f"   Sent:      {sent:,} events")
    print(f"   Errors:    {errors}")
    print(f"   Time:      {total_time:.2f}s")
    print(f"   Rate:      {final_rate:,.0f} events/sec")
    if remaining > 0:
        print(f"   ⚠️  {remaining} messages still in queue after flush")

    return {"sent": sent, "time_sec": total_time, "rate": final_rate}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Event Producer")
    parser.add_argument("--count", type=int, default=100_000,
                        help="Total events to produce (default: 100000)")
    parser.add_argument("--rate", type=int, default=0,
                        help="Target events/sec (0=unlimited)")
    parser.add_argument("--verbose", action="store_true",
                        help="Print delivery callbacks")
    args = parser.parse_args()

    run_producer(args.count, target_rate=args.rate, verbose=args.verbose)
