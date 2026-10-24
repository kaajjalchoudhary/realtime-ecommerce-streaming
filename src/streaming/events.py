"""Event model, validation and a synthetic event generator."""

import json
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

EVENT_TYPES = ("page_view", "add_to_cart", "purchase")
PRODUCTS = {f"SKU-{i:03d}": round(5 + i * 3.75, 2) for i in range(1, 51)}
REQUIRED_FIELDS = (
    "event_id",
    "event_type",
    "user_id",
    "product_id",
    "price",
    "quantity",
    "event_time",
)


class InvalidEvent(ValueError):
    """Raised when a message cannot be turned into a valid Event."""


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    user_id: int
    product_id: str
    price: float
    quantity: int
    event_time: datetime

    @property
    def revenue(self) -> float:
        return round(self.price * self.quantity, 2) if self.event_type == "purchase" else 0.0

    def to_json(self) -> bytes:
        payload = asdict(self)
        payload["event_time"] = self.event_time.isoformat()
        return json.dumps(payload).encode("utf-8")


def parse_event(raw: bytes) -> Event:
    """Decode and validate a raw Kafka message value."""
    try:
        data = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise InvalidEvent(f"malformed json: {exc}") from exc
    if not isinstance(data, dict):
        raise InvalidEvent("payload is not a JSON object")

    missing = [f for f in REQUIRED_FIELDS if data.get(f) is None]
    if missing:
        raise InvalidEvent(f"missing fields: {', '.join(missing)}")
    if data["event_type"] not in EVENT_TYPES:
        raise InvalidEvent(f"unknown event_type: {data['event_type']!r}")

    try:
        price = float(data["price"])
        quantity = int(data["quantity"])
        user_id = int(data["user_id"])
        event_time = datetime.fromisoformat(str(data["event_time"]))
    except (TypeError, ValueError) as exc:
        raise InvalidEvent(f"bad field type: {exc}") from exc

    if price < 0:
        raise InvalidEvent("negative price")
    if quantity <= 0:
        raise InvalidEvent("quantity must be positive")
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)

    return Event(
        event_id=str(data["event_id"]),
        event_type=data["event_type"],
        user_id=user_id,
        product_id=str(data["product_id"]),
        price=price,
        quantity=quantity,
        event_time=event_time.astimezone(timezone.utc),
    )


def generate_event(rng: random.Random, event_time: Optional[datetime] = None) -> Event:
    """A realistic funnel: most events are page views, few become purchases."""
    product_id = rng.choice(list(PRODUCTS))
    event_type = rng.choices(EVENT_TYPES, weights=[80, 15, 5])[0]
    return Event(
        event_id=str(uuid.UUID(int=rng.getrandbits(128))),
        event_type=event_type,
        user_id=rng.randint(1, 500),
        product_id=product_id,
        price=PRODUCTS[product_id],
        quantity=rng.randint(1, 3) if event_type != "page_view" else 1,
        event_time=event_time or datetime.now(timezone.utc),
    )


def corrupt(rng: random.Random, event: Event) -> bytes:
    """Produce a broken message so the dead-letter path gets exercised."""
    payload = json.loads(event.to_json())
    kind = rng.choice(["drop_field", "negative_price", "bad_type", "not_json"])
    if kind == "drop_field":
        payload.pop(rng.choice(REQUIRED_FIELDS))
    elif kind == "negative_price":
        payload["price"] = -payload["price"]
    elif kind == "bad_type":
        payload["event_type"] = "refund_requested"
    else:
        return b"{not valid json"
    return json.dumps(payload).encode("utf-8")
