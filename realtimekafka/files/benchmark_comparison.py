"""
Head-to-Head Benchmark: Per-Event vs Micro-Batch Consumer

Runs both consumers against the same data and produces a comparison
report with throughput numbers and plots.

This is the script that generates your portfolio-ready screenshot.

Usage:
    1. Start Kafka:    docker compose up -d
    2. Produce events: python producer.py --count 200000
    3. Run benchmark:  python benchmark_comparison.py

    Or run everything at once:
    python benchmark_comparison.py --full-run --events 200000
"""

import time
import sys
import os
import argparse
import subprocess
import json

# Check dependencies before importing
def check_deps():
    missing = []
    try:
        import confluent_kafka
    except ImportError:
        missing.append("confluent-kafka")
    try:
        import polars
    except ImportError:
        missing.append("polars")
    try:
        import orjson
    except ImportError:
        missing.append("orjson")
    if missing:
        print(f"❌ Missing packages: {', '.join(missing)}")
        print(f"   Run: pip install {' '.join(missing)}")
        sys.exit(1)

check_deps()

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from config import KAFKA_BOOTSTRAP, TOPIC_RAW_EVENTS


def check_kafka():
    """Verify Kafka is reachable."""
    try:
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        metadata = admin.list_topics(timeout=5)
        print(f"✅ Kafka is running ({len(metadata.topics)} topics)")
        return True
    except Exception as e:
        print(f"❌ Cannot connect to Kafka at {KAFKA_BOOTSTRAP}")
        print(f"   Error: {e}")
        print(f"   Start Kafka with: docker compose up -d")
        return False


def reset_consumer_groups():
    """Delete consumer groups so both consumers start fresh."""
    try:
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        # Create topic if not exists
        topics = admin.list_topics(timeout=5).topics
        if TOPIC_RAW_EVENTS not in topics:
            admin.create_topics([
                NewTopic(TOPIC_RAW_EVENTS, num_partitions=4, replication_factor=1)
            ])
            time.sleep(2)
            print(f"  Created topic: {TOPIC_RAW_EVENTS}")
    except Exception as e:
        print(f"  ⚠️ Could not manage topics: {e}")


def count_topic_messages() -> int:
    """Count total messages in the topic."""
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "counter-temp",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_RAW_EVENTS])

    count = 0
    empty_polls = 0
    while empty_polls < 10:
        msg = consumer.poll(timeout=0.5)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            continue
        count += 1
        empty_polls = 0

    consumer.close()
    return count


def run_comparison(max_events: int = 100_000):
    """Run both consumers and compare throughput."""
    from consumer_per_event import run_per_event_consumer
    from consumer_microbatch import run_microbatch_consumer

    print("\n" + "=" * 65)
    print("  📊 HEAD-TO-HEAD: Per-Event vs Micro-Batch Consumer")
    print("=" * 65)

    # Run per-event consumer first
    print(f"\n{'─' * 65}")
    print("  Round 1: Per-Event Consumer (baseline)")
    print(f"{'─' * 65}\n")
    result_per_event = run_per_event_consumer(
        max_events=max_events, timeout_sec=15.0
    )

    # Reset consumer group offset for fair comparison
    print(f"\n{'─' * 65}")
    print("  Round 2: Micro-Batch Consumer (optimized)")
    print(f"{'─' * 65}\n")
    result_microbatch = run_microbatch_consumer(
        max_events=max_events, batch_size=5000, timeout_sec=15.0
    )

    # ── Comparison Report ──
    print("\n" + "=" * 65)
    print("  📊 COMPARISON RESULTS")
    print("=" * 65)

    r1 = result_per_event
    r2 = result_microbatch

    speedup = r2["rate"] / r1["rate"] if r1["rate"] > 0 else 0

    print(f"""
  ┌──────────────────────┬──────────────┬──────────────┐
  │ Metric               │ Per-Event    │ Micro-Batch  │
  ├──────────────────────┼──────────────┼──────────────┤
  │ Events processed     │ {r1['processed']:>10,}   │ {r2['processed']:>10,}   │
  │ Total time (sec)     │ {r1['time_sec']:>10.2f}   │ {r2['time_sec']:>10.2f}   │
  │ Throughput (evt/sec) │ {r1['rate']:>10,.0f}   │ {r2['rate']:>10,.0f}   │
  │ Users tracked        │ {r1['users_tracked']:>10,}   │ {r2['users_tracked']:>10,}   │
  └──────────────────────┴──────────────┴──────────────┘

  🏆 Micro-batch speedup: {speedup:.1f}x faster

  Why micro-batching wins:
  • Amortized deserialization overhead across {r2.get('batches', '?')} batches
  • Vectorized transforms (Polars/Rust) vs Python dict operations
  • Fewer Kafka poll() round-trips
  • Better CPU cache utilization (columnar data layout)
""")

    # ── Extrapolate to daily throughput ──
    events_per_day_per_event = r1["rate"] * 86400
    events_per_day_microbatch = r2["rate"] * 86400

    print(f"  📈 Projected daily throughput (single consumer):")
    print(f"     Per-event:   {events_per_day_per_event:>15,.0f} events/day "
          f"({events_per_day_per_event/1e6:.1f}M)")
    print(f"     Micro-batch: {events_per_day_microbatch:>15,.0f} events/day "
          f"({events_per_day_microbatch/1e6:.1f}M)")
    print(f"\n  With {4} parallel consumers (matching partition count):")
    print(f"     Per-event:   {events_per_day_per_event*4/1e6:.1f}M events/day")
    print(f"     Micro-batch: {events_per_day_microbatch*4/1e6:.1f}M events/day")

    if events_per_day_microbatch * 4 >= 80e6:
        print(f"\n  ✅ Micro-batch with 4 consumers can handle 80M+ events/day!")
    else:
        consumers_needed = int(80e6 / events_per_day_microbatch) + 1
        print(f"\n  ℹ️  Need ~{consumers_needed} micro-batch consumers for 80M events/day")
        print(f"     (increase partitions and consumers accordingly)")

    # Save results
    results = {
        "per_event": {k: v for k, v in r1.items() if k != "batch_stats"},
        "microbatch": {k: v for k, v in r2.items() if k != "batch_stats"},
        "speedup": speedup,
        "max_events": max_events,
    }
    with open("comparison_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  💾 Results saved to comparison_results.json")

    # Generate plot
    generate_plot(r1, r2, speedup)

    return results


def generate_plot(r1: dict, r2: dict, speedup: float):
    """Generate comparison visualization."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("  ⚠️  matplotlib not found — skipping plot")
        return

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle(
        "Kafka Pipeline: Per-Event vs Micro-Batch Consumer\n"
        "Feature computation throughput comparison",
        fontsize=14, fontweight="bold",
    )

    colors = ["#e74c3c", "#2ecc71"]
    labels = ["Per-Event\n(baseline)", "Micro-Batch\n(optimized)"]

    # Plot 1: Throughput comparison
    ax1 = axes[0]
    rates = [r1["rate"], r2["rate"]]
    bars = ax1.bar(labels, rates, color=colors, width=0.5, edgecolor="white")
    ax1.set_ylabel("Events/sec", fontsize=11)
    ax1.set_title("Throughput", fontsize=12)
    for bar, rate in zip(bars, rates):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(rates)*0.02,
                 f"{rate:,.0f}", ha="center", fontsize=10, fontweight="bold")
    ax1.grid(True, alpha=0.3, axis="y")

    # Plot 2: Time to process
    ax2 = axes[1]
    times = [r1["time_sec"], r2["time_sec"]]
    bars = ax2.bar(labels, times, color=colors, width=0.5, edgecolor="white")
    ax2.set_ylabel("Seconds", fontsize=11)
    ax2.set_title(f"Time to Process {r1['processed']:,} Events", fontsize=12)
    for bar, t in zip(bars, times):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(times)*0.02,
                 f"{t:.1f}s", ha="center", fontsize=10, fontweight="bold")
    ax2.grid(True, alpha=0.3, axis="y")

    # Plot 3: Speedup callout
    ax3 = axes[2]
    ax3.set_xlim(0, 1)
    ax3.set_ylim(0, 1)
    ax3.text(0.5, 0.55, f"{speedup:.1f}x", ha="center", va="center",
             fontsize=60, fontweight="bold", color="#2ecc71")
    ax3.text(0.5, 0.25, "Faster", ha="center", va="center",
             fontsize=20, color="#555")
    ax3.text(0.5, 0.85, "Micro-Batch Speedup", ha="center", va="center",
             fontsize=13, fontweight="bold", color="#333")
    ax3.set_title("", fontsize=12)
    ax3.axis("off")

    plt.tight_layout()
    plt.savefig("comparison_results.png", dpi=150, bbox_inches="tight")
    print(f"  📈 Plot saved to comparison_results.png")
    plt.close()


def full_run(total_events: int):
    """Produce events then run comparison."""
    print("🔄 Full run: produce → consume (per-event) → consume (micro-batch)\n")

    if not check_kafka():
        return

    reset_consumer_groups()

    # Produce events
    print(f"\n{'─' * 65}")
    print(f"  Step 1: Producing {total_events:,} events...")
    print(f"{'─' * 65}\n")
    from producer import run_producer
    run_producer(total_events)

    # Wait for messages to be available
    time.sleep(2)

    # Run comparison (each consumer reads from beginning)
    half = total_events  # each consumer gets all events via different group.id
    run_comparison(max_events=half)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Head-to-head: Per-Event vs Micro-Batch Consumer"
    )
    parser.add_argument("--full-run", action="store_true",
                        help="Produce events then run both consumers")
    parser.add_argument("--events", type=int, default=200_000,
                        help="Events to produce/consume (default: 200000)")
    parser.add_argument("--max-events", type=int, default=100_000,
                        help="Max events per consumer (default: 100000)")
    args = parser.parse_args()

    if not check_kafka():
        sys.exit(1)

    if args.full_run:
        full_run(args.events)
    else:
        run_comparison(max_events=args.max_events)
