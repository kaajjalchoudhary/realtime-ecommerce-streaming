"""Print the latest window metrics; exits non-zero if nothing has been written.

python -m streaming.report --limit 10
"""

import argparse
import sys

import psycopg

from streaming.config import Settings

QUERY = """
SELECT window_start, page_views, add_to_carts, purchases, revenue, unique_users, conversion_rate
FROM window_metrics
ORDER BY window_start DESC
LIMIT %s
"""


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--limit", type=int, default=10)
    p.add_argument("--min-windows", type=int, default=1)
    a = p.parse_args()

    with psycopg.connect(Settings().postgres_dsn) as conn:
        total = conn.execute("SELECT count(*) FROM window_metrics").fetchone()[0]
        rows = conn.execute(QUERY, (a.limit,)).fetchall()

    header = (
        f"{'window_start':<20}{'views':>7}{'carts':>7}{'orders':>8}"
        f"{'revenue':>11}{'users':>7}{'conv':>8}"
    )
    print(header)
    print("-" * len(header))
    for start, views, carts, orders, revenue, users, conv in rows:
        print(
            f"{start:%Y-%m-%d %H:%M}    {views:>7}{carts:>7}{orders:>8}"
            f"{revenue:>11.2f}{users:>7}{conv:>8.2%}"
        )
    print(f"\n{total} windows stored")
    if total < a.min_windows:
        sys.exit(f"Expected at least {a.min_windows} windows, found {total}")


if __name__ == "__main__":
    main()
