from datetime import datetime, timedelta, timezone

import pytest

from streaming.events import Event
from streaming.windowing import TumblingWindowAggregator

T0 = datetime(2024, 5, 1, 12, 0, 0, tzinfo=timezone.utc)


def ev(seconds, event_type="page_view", user_id=1, price=10.0, quantity=1):
    return Event(
        event_id=f"{seconds}-{event_type}-{user_id}",
        event_type=event_type,
        user_id=user_id,
        product_id="SKU-001",
        price=price,
        quantity=quantity,
        event_time=T0 + timedelta(seconds=seconds),
    )


def test_window_start_alignment():
    agg = TumblingWindowAggregator(window_seconds=60)
    assert agg.window_start(T0 + timedelta(seconds=59)) == T0
    assert agg.window_start(T0 + timedelta(seconds=60)) == T0 + timedelta(minutes=1)


def test_aggregates_counts_revenue_and_users():
    agg = TumblingWindowAggregator(60, 0)
    for e in [
        ev(1, user_id=1),
        ev(2, user_id=2),
        ev(3, "add_to_cart", user_id=2),
        ev(4, "purchase", user_id=2, price=12.5, quantity=2),
    ]:
        agg.add(e)
    (window,) = agg.pop_all()
    assert (window.page_views, window.add_to_carts, window.purchases) == (2, 1, 1)
    assert window.revenue == 25.0
    assert window.unique_users == 2
    assert window.conversion_rate == 0.5


def test_window_closes_only_after_watermark_passes():
    agg = TumblingWindowAggregator(60, allowed_lateness_seconds=30)
    agg.add(ev(10))
    agg.add(ev(70))  # watermark = 40s, first window ends at 60s
    assert agg.pop_closed() == []
    agg.add(ev(95))  # watermark = 65s
    closed = agg.pop_closed()
    assert [w.window_start for w in closed] == [T0]
    assert agg.open_windows == 1


def test_out_of_order_event_within_lateness_is_kept():
    agg = TumblingWindowAggregator(60, allowed_lateness_seconds=30)
    agg.add(ev(10))
    agg.add(ev(80))
    assert agg.add(ev(50))  # older, but its window has not been emitted yet
    agg.add(ev(100))
    (first,) = agg.pop_closed()
    assert first.page_views == 2


def test_event_for_emitted_window_is_dropped_as_late():
    agg = TumblingWindowAggregator(60, allowed_lateness_seconds=0)
    agg.add(ev(10))
    agg.add(ev(65))
    assert len(agg.pop_closed()) == 1
    assert agg.add(ev(20)) is False
    assert agg.late_events == 1


def test_pop_all_flushes_in_order():
    agg = TumblingWindowAggregator(60, 0)
    for s in (130, 10, 70):
        agg.add(ev(s))
    starts = [w.window_start for w in agg.pop_all()]
    assert starts == sorted(starts) and len(starts) == 3
    assert agg.open_windows == 0


def test_invalid_window_size():
    with pytest.raises(ValueError):
        TumblingWindowAggregator(0)


def test_watermark_is_minimum_across_partitions():
    agg = TumblingWindowAggregator(60, allowed_lateness_seconds=0)
    agg.set_partitions([0, 1])
    agg.add(ev(10), partition=0)
    agg.add(ev(500), partition=0)  # partition 0 is read far ahead
    assert agg.watermark is None  # partition 1 has not reported yet
    assert agg.pop_closed() == []

    assert agg.add(ev(20), partition=1)  # not late: partition 1 is still at t=20
    agg.add(ev(130), partition=1)
    closed = agg.pop_closed()  # watermark = min(500, 130) = 130
    assert [w.window_start for w in closed] == [T0]
    assert closed[0].page_views == 2
    assert agg.late_events == 0


def test_rebalance_drops_revoked_partitions():
    agg = TumblingWindowAggregator(60, 0)
    agg.set_partitions([0, 1])
    agg.add(ev(10), partition=0)
    agg.add(ev(200), partition=1)
    agg.set_partitions([1])
    assert agg.watermark == T0 + timedelta(seconds=200)
