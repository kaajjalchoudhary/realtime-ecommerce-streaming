"""Publish synthetic e-commerce events to Kafka.

python -m streaming.producer --events 5000 --rate 200
python -m streaming.producer --events 3000 --simulate-minutes 15   # backfill-style demo
"""

import argparse
import logging
import random
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from streaming.config import Settings
from streaming.events import corrupt, generate_event

log = logging.getLogger("producer")


def ensure_topics(settings: Settings, partitions: int = 3) -> None:
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics
    wanted = [
        NewTopic(t, num_partitions=partitions, replication_factor=1)
        for t in (settings.events_topic, settings.dlq_topic)
        if t not in existing
    ]
    if not wanted:
        return
    for topic, future in admin.create_topics(wanted).items():
        try:
            future.result()
            log.info("Created topic %s", topic)
        except KafkaException as exc:
            log.warning("Topic %s: %s", topic, exc)


def run(events: int, rate: float, invalid_ratio: float, simulate_minutes: float, seed: int) -> dict:
    settings = Settings()
    ensure_topics(settings)
    producer = Producer({"bootstrap.servers": settings.bootstrap_servers, "linger.ms": 20})
    rng = random.Random(seed)
    stats = {"sent": 0, "invalid": 0, "failed": 0}

    def on_delivery(err, _msg):
        if err is not None:
            stats["failed"] += 1
            log.error("Delivery failed: %s", err)

    start = datetime.now(timezone.utc) - timedelta(minutes=simulate_minutes)
    step = timedelta(seconds=simulate_minutes * 60 / events) if simulate_minutes else None

    for i in range(events):
        event_time = start + i * step if step else None
        event = generate_event(rng, event_time)
        if rng.random() < invalid_ratio:
            value = corrupt(rng, event)
            stats["invalid"] += 1
        else:
            value = event.to_json()
        producer.produce(
            settings.events_topic,
            key=str(event.user_id).encode(),
            value=value,
            on_delivery=on_delivery,
        )
        producer.poll(0)
        stats["sent"] += 1
        if rate:
            time.sleep(1 / rate)

    producer.flush(30)
    log.info("Produced %s", stats)
    return stats


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Synthetic e-commerce event producer")
    p.add_argument("--events", type=int, default=1000)
    p.add_argument("--rate", type=float, default=0, help="events per second, 0 = unthrottled")
    p.add_argument("--invalid-ratio", type=float, default=0.02)
    p.add_argument(
        "--simulate-minutes",
        type=float,
        default=0,
        help="spread event times over the last N minutes instead of using 'now'",
    )
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    run(a.events, a.rate, a.invalid_ratio, a.simulate_minutes, a.seed)


if __name__ == "__main__":
    main()
