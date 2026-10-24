import json
import random
from datetime import datetime, timezone

import pytest

from streaming.events import InvalidEvent, corrupt, generate_event, parse_event


def _payload(**overrides):
    data = {
        "event_id": "e-1",
        "event_type": "purchase",
        "user_id": 7,
        "product_id": "SKU-001",
        "price": 8.75,
        "quantity": 2,
        "event_time": "2024-05-01T12:00:30+00:00",
    }
    data.update(overrides)
    return json.dumps(data).encode()


def test_parse_valid_event():
    event = parse_event(_payload())
    assert event.user_id == 7
    assert event.revenue == 17.5
    assert event.event_time == datetime(2024, 5, 1, 12, 0, 30, tzinfo=timezone.utc)


def test_naive_timestamps_are_treated_as_utc():
    event = parse_event(_payload(event_time="2024-05-01T12:00:30"))
    assert event.event_time.tzinfo == timezone.utc


def test_non_purchase_has_no_revenue():
    assert parse_event(_payload(event_type="page_view")).revenue == 0.0


@pytest.mark.parametrize(
    "raw, reason",
    [
        (b"{oops", "malformed json"),
        (b"[1, 2]", "not a JSON object"),
        (_payload(user_id=None), "missing fields: user_id"),
        (_payload(event_type="refund"), "unknown event_type"),
        (_payload(price=-1), "negative price"),
        (_payload(quantity=0), "quantity must be positive"),
        (_payload(price="abc"), "bad field type"),
    ],
)
def test_invalid_events_are_rejected(raw, reason):
    with pytest.raises(InvalidEvent, match=reason):
        parse_event(raw)


def test_generated_events_round_trip():
    rng = random.Random(1)
    for _ in range(100):
        event = generate_event(rng)
        assert parse_event(event.to_json()) == event


def test_corrupted_events_never_parse():
    rng = random.Random(2)
    for _ in range(100):
        with pytest.raises(InvalidEvent):
            parse_event(corrupt(rng, generate_event(rng)))
