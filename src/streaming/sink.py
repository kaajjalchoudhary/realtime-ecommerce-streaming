"""PostgreSQL sink for window metrics (idempotent upserts)."""

from typing import Iterable

import psycopg

from streaming.windowing import WindowStats

UPSERT_SQL = """
INSERT INTO window_metrics (
    window_start, window_end, page_views, add_to_carts, purchases,
    revenue, unique_users, conversion_rate
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (window_start) DO UPDATE SET
    window_end = EXCLUDED.window_end,
    page_views = EXCLUDED.page_views,
    add_to_carts = EXCLUDED.add_to_carts,
    purchases = EXCLUDED.purchases,
    revenue = EXCLUDED.revenue,
    unique_users = EXCLUDED.unique_users,
    conversion_rate = EXCLUDED.conversion_rate,
    updated_at = now()
"""


def to_row(w: WindowStats) -> tuple:
    return (
        w.window_start,
        w.window_end,
        w.page_views,
        w.add_to_carts,
        w.purchases,
        w.revenue,
        w.unique_users,
        w.conversion_rate,
    )


class PostgresSink:
    def __init__(self, dsn: str):
        self.conn = psycopg.connect(dsn)

    def write(self, windows: Iterable[WindowStats]) -> int:
        rows = [to_row(w) for w in windows]
        if not rows:
            return 0
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.executemany(UPSERT_SQL, rows)
        return len(rows)

    def close(self) -> None:
        self.conn.close()
