"""
Micro-Batch Consumer (OPTIMIZED) — Process events in vectorized batches.

This is the FAST way and the core of the resume bullet:
  "Real-time data ingestion scale-up to 80M+ events/day via Kafka
   micro-batching and vectorized feature transforms"

Key differences from per-event:
  1. Poll batches of events (not one at a time)
  2. Deserialize batch → columnar DataFrame (Polars, Arrow-backed)
  3. Feature transforms are vectorized column ops (no Python loops)
  4. Bulk output writes (not per-event)

Usage:
    python consumer_microbatch.py
    python consumer_microbatch.py --max-events 100000 --batch-size 5000
"""

import time
import sys
import argparse
import signal
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError
import orjson
import polars as pl

from config import KAFKA_BOOTSTRAP, TOPIC_RAW_EVENTS


# ─── Micro-Batch Feature Pipeline ───────────────────────────

class MicroBatchFeaturePipeline:
    """
    Vectorized feature computation using Polars.

    For each micro-batch, we compute user-level aggregates using
    columnar operations — no Python loops over individual events.

    In production, this is where you'd compute:
    - Rolling window features (clicks in last 5 min)
    - Session-level aggregates
    - Real-time user profiles
    - Anomaly detection signals

    The results would be written to:
    - Online feature store (Redis/DynamoDB) for serving
    - Offline store (Parquet/Delta) for training
    """
    def __init__(self):
        # Accumulated user stats (in production: feature store writes)
        self.user_features = {}
        self.total_events_processed = 0
        self.total_batches = 0

    def process_batch(self, raw_messages: list) -> dict:
        """
        Process a micro-batch of raw Kafka messages.

        This is the heart of the optimization:
        - Deserialize once per batch (not per event)
        - Convert to columnar Polars DataFrame
        - All transforms are vectorized C++/Rust operations
        - Single bulk output

        Returns batch-level stats for monitoring.
        """
        if not raw_messages:
            return {}

        t0 = time.perf_counter()

        # ── Step 1: Batch deserialization ──
        # orjson is ~10x faster than stdlib json
        rows = [orjson.loads(msg) for msg in raw_messages]
        t_deser = time.perf_counter()

        # ── Step 2: Convert to columnar DataFrame ──
        # This is the key: Polars stores data in Apache Arrow format
        # (columnar, cache-friendly, SIMD-optimized)
        df = pl.DataFrame(rows)
        t_df = time.perf_counter()

        # ── Step 3: Vectorized feature transforms ──
        # All of these run in Rust/C++ — no Python loops

        # 3a. Time bucketing (minute-level for near-real-time features)
        df = df.with_columns([
            (pl.col("ts_ms") // 60_000).alias("minute_bucket"),
        ])

        # 3b. User-level aggregates per minute (vectorized groupby)
        user_features = (
            df.group_by(["user_id", "minute_bucket"])
            .agg([
                # Event counts
                pl.len().alias("events_1m"),
                (pl.col("event_type") == "click").sum().alias("clicks_1m"),
                (pl.col("event_type") == "view").sum().alias("views_1m"),
                (pl.col("event_type") == "purchase").sum().alias("purchases_1m"),
                (pl.col("event_type") == "search").sum().alias("searches_1m"),

                # Monetary
                pl.col("amount").sum().alias("total_spend_1m"),
                pl.col("amount").filter(pl.col("amount") > 0).mean().alias("avg_purchase_1m"),

                # Diversity signals
                pl.col("item_id").n_unique().alias("unique_items_1m"),
                pl.col("device").n_unique().alias("unique_devices_1m"),

                # Recency
                pl.col("ts_ms").max().alias("last_event_ts"),
                pl.col("ts_ms").min().alias("first_event_ts"),
            ])
        )

        # 3c. Derived features (still vectorized)
        user_features = user_features.with_columns([
            # Click-through rate proxy
            (pl.col("clicks_1m") / pl.col("views_1m").clip(lower_bound=1))
                .alias("ctr_1m"),

            # Session duration proxy (ms)
            (pl.col("last_event_ts") - pl.col("first_event_ts"))
                .alias("session_duration_ms"),

            # Activity intensity (events per second)
            (pl.col("events_1m") * 1000.0 /
             (pl.col("last_event_ts") - pl.col("first_event_ts")).clip(lower_bound=1))
                .alias("events_per_sec"),
        ])

        # 3d. Fill nulls (vectorized)
        user_features = user_features.fill_null(0)
        user_features = user_features.fill_nan(0)

        t_transform = time.perf_counter()

        # ── Step 4: "Write" to feature store (in production: bulk Redis/DynamoDB write) ──
        # Here we just accumulate in memory as a demo
        for row in user_features.iter_rows(named=True):
            uid = row["user_id"]
            self.user_features[uid] = row

        t_write = time.perf_counter()

        self.total_events_processed += len(raw_messages)
        self.total_batches += 1

        return {
            "batch_size": len(raw_messages),
            "users_in_batch": user_features.height,
            "features_computed": len(user_features.columns),
            "time_deser_ms": (t_deser - t0) * 1000,
            "time_to_df_ms": (t_df - t_deser) * 1000,
            "time_transform_ms": (t_transform - t_df) * 1000,
            "time_write_ms": (t_write - t_transform) * 1000,
            "time_total_ms": (t_write - t0) * 1000,
        }

    @property
    def num_users(self):
        return len(self.user_features)


def run_microbatch_consumer(
    max_events: int = 0,
    batch_size: int = 5000,
    batch_timeout_ms: int = 500,
    timeout_sec: float = 30.0,
):
    """
    Consume events in micro-batches with vectorized processing.

    Args:
        max_events:       stop after N events (0 = until timeout)
        batch_size:       max events per micro-batch
        batch_timeout_ms: max wait time to fill a batch (ms)
        timeout_sec:      stop after N seconds of no messages
    """
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "microbatch-consumer",
        "auto.offset.reset": "earliest",

        # ── Consumer tuning for micro-batching ──
        # fetch.min.bytes: wait until at least this much data is available
        "fetch.min.bytes": 65536,          # 64KB minimum fetch

        # fetch.max.wait.ms: max wait for fetch.min.bytes
        "fetch.wait.max.ms": 200,

        # max.partition.fetch.bytes: max data per partition per fetch
        "max.partition.fetch.bytes": 1048576,  # 1MB

        "max.poll.interval.ms": 300000,
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    }

    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_RAW_EVENTS])

    pipeline = MicroBatchFeaturePipeline()

    print(f"⚡ Micro-Batch Consumer started (optimized)")
    print(f"   Batch size: {batch_size:,} events")
    print(f"   Batch timeout: {batch_timeout_ms}ms")
    print(f"   Vectorized transforms via Polars (Arrow-backed)")
    print(f"   Topic: {TOPIC_RAW_EVENTS}")
    if max_events > 0:
        print(f"   Max events: {max_events:,}")
    print()

    processed = 0
    errors = 0
    t_start = time.perf_counter()
    t_last_report = t_start
    last_message_time = t_start
    batch_stats_history = []

    # Graceful shutdown
    running = True
    def handle_signal(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, handle_signal)

    try:
        while running:
            # ── Collect a micro-batch ──
            batch = []
            batch_start = time.perf_counter()

            while len(batch) < batch_size:
                remaining_ms = batch_timeout_ms - (time.perf_counter() - batch_start) * 1000
                if remaining_ms <= 0:
                    break

                msg = consumer.poll(timeout=min(remaining_ms / 1000, 0.5))

                if msg is None:
                    if time.perf_counter() - last_message_time > timeout_sec:
                        running = False
                        break
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        errors += 1
                    continue

                last_message_time = time.perf_counter()
                batch.append(msg.value())

            # ── Process the micro-batch (vectorized) ──
            if batch:
                stats = pipeline.process_batch(batch)
                processed += len(batch)
                batch_stats_history.append(stats)

                # Progress report
                now = time.perf_counter()
                if now - t_last_report >= 3.0:
                    elapsed = now - t_start
                    rate = processed / elapsed if elapsed > 0 else 0
                    print(f"  ⚡ {processed:>10,} events  "
                          f"({rate:,.0f} events/sec)  "
                          f"[batch={stats['batch_size']}, "
                          f"transform={stats['time_transform_ms']:.1f}ms, "
                          f"{pipeline.num_users:,} users]")
                    t_last_report = now

            if max_events > 0 and processed >= max_events:
                break

    finally:
        consumer.close()

    t_end = time.perf_counter()
    total_time = t_end - t_start
    final_rate = processed / total_time if total_time > 0 else 0

    # Compute average batch processing breakdown
    if batch_stats_history:
        avg_deser = sum(s["time_deser_ms"] for s in batch_stats_history) / len(batch_stats_history)
        avg_transform = sum(s["time_transform_ms"] for s in batch_stats_history) / len(batch_stats_history)
        avg_write = sum(s["time_write_ms"] for s in batch_stats_history) / len(batch_stats_history)
        avg_total = sum(s["time_total_ms"] for s in batch_stats_history) / len(batch_stats_history)
    else:
        avg_deser = avg_transform = avg_write = avg_total = 0

    print()
    print(f"⚡ Micro-Batch Consumer finished")
    print(f"   Processed:  {processed:,} events in {pipeline.total_batches} batches")
    print(f"   Errors:     {errors}")
    print(f"   Time:       {total_time:.2f}s")
    print(f"   Throughput: {final_rate:,.0f} events/sec")
    print(f"   Users:      {pipeline.num_users:,}")
    print(f"\n   Avg batch timing breakdown:")
    print(f"     Deserialize:  {avg_deser:.1f} ms")
    print(f"     Transform:    {avg_transform:.1f} ms  (vectorized Polars)")
    print(f"     Write:        {avg_write:.1f} ms")
    print(f"     Total:        {avg_total:.1f} ms per batch")

    return {
        "method": "microbatch",
        "processed": processed,
        "time_sec": total_time,
        "rate": final_rate,
        "users_tracked": pipeline.num_users,
        "batches": pipeline.total_batches,
        "avg_batch_ms": avg_total,
        "batch_stats": batch_stats_history,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Micro-Batch Consumer (Optimized)")
    parser.add_argument("--max-events", type=int, default=0,
                        help="Stop after N events (0=until timeout)")
    parser.add_argument("--batch-size", type=int, default=5000,
                        help="Max events per micro-batch")
    parser.add_argument("--batch-timeout", type=int, default=500,
                        help="Max ms to wait for a full batch")
    parser.add_argument("--timeout", type=float, default=15.0,
                        help="Stop after N seconds of no messages")
    args = parser.parse_args()

    run_microbatch_consumer(
        args.max_events, args.batch_size, args.batch_timeout, args.timeout
    )
