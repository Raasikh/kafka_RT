"""
Shared configuration and event schema for the Kafka pipeline.

Event Schema (what a typical ML feature pipeline ingests):
- user_id:    who did this
- event_type: what happened (click, view, purchase, search, etc.)
- item_id:    what item was involved
- amount:     monetary value (if applicable)
- ts_ms:      unix timestamp in milliseconds
- device:     mobile / desktop / tablet
- region:     geographic region
"""

import time
import random
import string
import orjson
from dataclasses import dataclass, asdict
from typing import Optional


# ─── Kafka Config ───────────────────────────────────────────

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_RAW_EVENTS = "raw_events"
TOPIC_FEATURES = "user_features"  # output topic for computed features
NUM_PARTITIONS = 4


# ─── Event Types & Weights (realistic distribution) ─────────

EVENT_TYPES = {
    "view":     0.45,   # most common
    "click":    0.25,
    "search":   0.12,
    "add_cart": 0.08,
    "purchase": 0.05,
    "refund":   0.02,
    "share":    0.03,
}

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.55, 0.35, 0.10]

REGIONS = ["us-east", "us-west", "eu-west", "eu-east", "apac"]


# ─── Event Generation ───────────────────────────────────────

@dataclass
class RawEvent:
    user_id: str
    event_type: str
    item_id: str
    amount: float
    ts_ms: int
    device: str
    region: str

    def to_json_bytes(self) -> bytes:
        """Serialize to JSON bytes using orjson (fast)."""
        return orjson.dumps(asdict(self))

    @staticmethod
    def from_json_bytes(data: bytes) -> "RawEvent":
        d = orjson.loads(data)
        return RawEvent(**d)


def generate_event() -> RawEvent:
    """Generate a realistic-looking user event."""
    event_type = random.choices(
        list(EVENT_TYPES.keys()),
        weights=list(EVENT_TYPES.values()),
    )[0]

    # Amount only for purchase/refund
    amount = 0.0
    if event_type == "purchase":
        amount = round(random.uniform(5.0, 500.0), 2)
    elif event_type == "refund":
        amount = -round(random.uniform(5.0, 200.0), 2)

    return RawEvent(
        user_id=f"u{random.randint(1, 10000):05d}",
        event_type=event_type,
        item_id=f"item_{random.randint(1, 50000):06d}",
        amount=amount,
        ts_ms=int(time.time() * 1000),
        device=random.choices(DEVICES, weights=DEVICE_WEIGHTS)[0],
        region=random.choice(REGIONS),
    )
