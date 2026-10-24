"""Event-time tumbling windows with a watermark for late data.

Each window is emitted exactly once, when the watermark passes the window end.
Like Flink, the watermark is tracked per Kafka partition and the overall
watermark is the *minimum* across partitions (minus the allowed lateness), so a
partition that is read ahead of the others cannot make their events "late".
Events that arrive for a window that has already been emitted are counted as
late and dropped.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Set

from streaming.events import Event

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass
class WindowStats:
    window_start: datetime
    window_end: datetime
    page_views: int = 0
    add_to_carts: int = 0
    purchases: int = 0
    revenue: float = 0.0
    users: Set[int] = field(default_factory=set)

    def add(self, event: Event) -> None:
        if event.event_type == "page_view":
            self.page_views += 1
        elif event.event_type == "add_to_cart":
            self.add_to_carts += 1
        else:
            self.purchases += 1
            self.revenue = round(self.revenue + event.revenue, 2)
        self.users.add(event.user_id)

    @property
    def unique_users(self) -> int:
        return len(self.users)

    @property
    def conversion_rate(self) -> float:
        return round(self.purchases / self.page_views, 4) if self.page_views else 0.0


class TumblingWindowAggregator:
    def __init__(self, window_seconds: int = 60, allowed_lateness_seconds: int = 30):
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.window = timedelta(seconds=window_seconds)
        self.lateness = timedelta(seconds=allowed_lateness_seconds)
        self._open: Dict[datetime, WindowStats] = {}
        self._max_by_partition: Dict[int, datetime] = {}
        self._partitions: Optional[Set[int]] = None
        self._emitted_until: Optional[datetime] = None
        self.late_events = 0

    def window_start(self, ts: datetime) -> datetime:
        offset = (ts - EPOCH) // self.window
        return EPOCH + offset * self.window

    def set_partitions(self, partitions: Iterable[int]) -> None:
        """Declare the partitions this consumer owns (call on rebalance).

        Until every owned partition has delivered at least one event the
        watermark stays undefined, so no window is closed prematurely.
        """
        self._partitions = set(partitions)
        for p in list(self._max_by_partition):
            if p not in self._partitions:
                del self._max_by_partition[p]

    @property
    def watermark(self) -> Optional[datetime]:
        expected = self._partitions if self._partitions is not None else self._max_by_partition
        if not expected or any(p not in self._max_by_partition for p in expected):
            return None
        return min(self._max_by_partition[p] for p in expected) - self.lateness

    def add(self, event: Event, partition: int = 0) -> bool:
        """Add an event read from `partition`. Returns False if it was dropped as late."""
        start = self.window_start(event.event_time)
        if self._emitted_until is not None and start < self._emitted_until:
            self.late_events += 1
            return False

        stats = self._open.get(start)
        if stats is None:
            stats = self._open[start] = WindowStats(start, start + self.window)
        stats.add(event)

        current = self._max_by_partition.get(partition)
        if current is None or event.event_time > current:
            self._max_by_partition[partition] = event.event_time
        return True

    def pop_closed(self) -> List[WindowStats]:
        """Remove and return every window whose end is at or before the watermark."""
        wm = self.watermark
        if wm is None:
            return []
        closed = sorted(
            (w for w in self._open.values() if w.window_end <= wm), key=lambda w: w.window_start
        )
        for w in closed:
            del self._open[w.window_start]
            self._emitted_until = max(self._emitted_until or w.window_end, w.window_end)
        return closed

    def pop_all(self) -> List[WindowStats]:
        """Flush everything (used on shutdown)."""
        remaining = sorted(self._open.values(), key=lambda w: w.window_start)
        self._open.clear()
        if remaining:
            self._emitted_until = max(
                self._emitted_until or remaining[-1].window_end, remaining[-1].window_end
            )
        return remaining

    @property
    def open_windows(self) -> int:
        return len(self._open)
