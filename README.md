# Kafka Real-Time Feature Pipeline

## What This Is

A complete, runnable Kafka pipeline that demonstrates:
- **High-throughput event production** with tuned Kafka producer settings
- **Per-event processing** (baseline — the slow way)
- **Micro-batch processing** with vectorized transforms (optimized — the fast way)
- **Head-to-head throughput comparison** with metrics and plots

This is a hands-on implementation of:
> *"Real-time data ingestion scale-up to 80M+ events/day via Kafka micro-batching and vectorized feature transforms"*

## Architecture

```
┌─────────────┐     ┌─────────┐     ┌─────────────────────┐     ┌───────────────┐
│  Producer    │────▶│  Kafka  │────▶│  Consumer           │────▶│ Feature Store  │
│  (app events)│     │  Topic  │     │  (micro-batch +     │     │ (Redis/Parquet)│
│              │     │ 4 parts │     │   Polars vectorized) │     │               │
└─────────────┘     └─────────┘     └─────────────────────┘     └───────────────┘

Events: user_id, event_type, item_id, amount, timestamp, device, region
Features: clicks_1m, views_1m, ctr_1m, total_spend, session_duration, ...
```

## Quick Start (5 minutes)

### 1. Start Kafka
```bash
docker compose up -d
```
Wait ~10 seconds for Kafka to be ready.

### 2. Install Python deps
```bash
pip install confluent-kafka polars orjson matplotlib tabulate rich
```

### 3. Run the full benchmark
```bash
# Option A: Full automated run (produce + both consumers + comparison)
python benchmark_comparison.py --full-run --events 200000

# Option B: Step by step
python producer.py --count 200000          # produce events
python consumer_per_event.py --max-events 100000   # slow baseline
python consumer_microbatch.py --max-events 100000  # fast optimized
python benchmark_comparison.py             # compare results
```

### 4. Screenshot results
The benchmark prints a comparison table and saves `comparison_results.png`.

### 5. Cleanup
```bash
docker compose down
```

## Project Structure

```
kafka_pipeline/
├── docker-compose.yml          # Kafka + Zookeeper (one command)
├── requirements.txt            # Python dependencies
├── config.py                   # Shared config, event schema, generators
├── producer.py                 # High-throughput event producer
├── consumer_per_event.py       # BASELINE: process one event at a time
├── consumer_microbatch.py      # OPTIMIZED: micro-batch + Polars vectorized
├── benchmark_comparison.py     # Head-to-head comparison + plots
└── README.md                   # This file
```

## What Each File Does

### `producer.py` — Event Producer
Simulates application traffic publishing to Kafka. Tuned for throughput:
- `batch.size=64KB` — batch messages before sending
- `linger.ms=10` — wait 10ms to fill batches
- `compression.type=lz4` — compress on the wire
- Partitions by `user_id` for ordering guarantees

### `consumer_per_event.py` — Baseline (Slow)
Processes events **one at a time** using Python dicts:
```
for each event:
    deserialize JSON          # overhead per event
    update Python dict        # no vectorization
    individual dict lookups   # cache-unfriendly
```

### `consumer_microbatch.py` — Optimized (Fast)
Processes events in **micro-batches** using Polars:
```
collect batch of N events:
    deserialize all at once   # amortized overhead
    → Polars DataFrame        # columnar, Arrow-backed
    vectorized groupby/agg    # Rust/C++, SIMD
    bulk write features       # single operation
```

The key insight: feature transforms like `groupby → agg → derived columns` run in
Rust/C++ when expressed as Polars column operations, vs slow Python per-event.

## Expected Results

On a typical laptop (M1/M2 Mac or modern x86):

| Metric | Per-Event | Micro-Batch |
|--------|-----------|-------------|
| Throughput | ~5k-15k events/sec | ~30k-80k+ events/sec |
| Speedup | baseline | **3-6x faster** |

With 4 parallel micro-batch consumers (one per partition):
- Single consumer: ~50k events/sec → ~4.3B events/day
- 4 consumers: ~200k events/sec → **17B+ events/day**

Even conservatively, this easily handles 80M+ events/day.

## Key Concepts for Interviews

### Why micro-batching beats per-event processing

| Per-Event (slow) | Micro-Batch (fast) |
|---|---|
| Deserialize 1 JSON per call | Deserialize N at once, amortize overhead |
| Python dict updates | Polars columnar ops (Rust/C++, SIMD) |
| Many small Kafka polls | Fewer, larger polls |
| Cache-unfriendly (random dict access) | Cache-friendly (columnar layout) |
| 1 "write" per event | Bulk write per batch |

### Why NOT fully streaming (record-by-record)?
> "Per-event has too much fixed overhead: per-record deserialization, Python function calls, object allocation. Micro-batching amortizes this across thousands of events while keeping end-to-end latency within SLA (sub-second)."

### What about Spark Structured Streaming?
> "Same principle — each micro-batch is a DataFrame. We could use Spark for heavier transforms or when data volume requires distributed processing. For this scale, single-node Polars was sufficient and simpler to operate."

### How to narrate in an interview

> "Our feature pipeline was falling behind as event volume grew. Per-event processing had too much Python overhead — deserialization, dict lookups, and individual writes per record. I switched to Kafka micro-batching: poll batches of 5000 events, convert to a Polars DataFrame (Arrow-backed columnar format), and run all feature transforms as vectorized operations in Rust. The same groupbys, aggregations, and derived features that took seconds per-event finished in milliseconds per batch. That let us scale from ~15k to 80k+ events/sec per consumer, and with parallel consumers matching our partition count, we comfortably handled 80M+ events/day while keeping feature freshness under our SLA."

## Producer Tuning Explained

| Setting | Value | Why |
|---------|-------|-----|
| `batch.size` | 64KB | Batch messages before send → fewer network calls |
| `linger.ms` | 10 | Wait 10ms to fill batch → higher throughput |
| `compression.type` | lz4 | Compress batches → less network I/O |
| `buffer.memory` | 64MB | Large buffer for async production |

## Consumer Tuning Explained

| Setting | Value | Why |
|---------|-------|-----|
| `fetch.min.bytes` | 64KB | Don't return tiny fetches → fewer round trips |
| `fetch.wait.max.ms` | 200ms | Max wait for min bytes → bounded latency |
| `max.partition.fetch.bytes` | 1MB | Large fetches per partition → more data per poll |
