"""Consume events, aggregate them into tumbling windows and persist closed windows.

python -m streaming.consumer                  # run forever
python -m streaming.consumer --idle-timeout 15  # stop after 15s without messages
"""

import argparse
import logging
import signal
import time
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from streaming.config import Settings
from streaming.events import InvalidEvent, parse_event
from streaming.sink import PostgresSink
from streaming.windowing import TumblingWindowAggregator

log = logging.getLogger("consumer")


class WindowedConsumer:
    def __init__(self, settings: Settings, sink):
        self.settings = settings
        self.sink = sink
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "group.id": settings.consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.dlq = Producer({"bootstrap.servers": settings.bootstrap_servers})
        self.aggregator = TumblingWindowAggregator(
            settings.window_seconds, settings.allowed_lateness_seconds
        )
        self.metrics = {"consumed": 0, "valid": 0, "dead_lettered": 0, "windows_written": 0}
        self._running = True

    def stop(self, *_args) -> None:
        self._running = False

    def _dead_letter(self, value: bytes, reason: str) -> None:
        self.dlq.produce(
            self.settings.dlq_topic, value=value, headers=[("error", reason.encode("utf-8"))]
        )
        self.dlq.poll(0)
        self.metrics["dead_lettered"] += 1

    def _flush(self, final: bool = False) -> None:
        windows = self.aggregator.pop_all() if final else self.aggregator.pop_closed()
        if windows:
            self.metrics["windows_written"] += self.sink.write(windows)
            for w in windows:
                log.info(
                    "window %s  views=%d carts=%d purchases=%d revenue=%.2f users=%d",
                    w.window_start.strftime("%Y-%m-%d %H:%M"),
                    w.page_views,
                    w.add_to_carts,
                    w.purchases,
                    w.revenue,
                    w.unique_users,
                )
        # Offsets are committed only after closed windows are durable in Postgres.
        if self.metrics["consumed"]:
            try:
                self.consumer.commit(asynchronous=False)
            except KafkaException as exc:
                if exc.args[0].code() != KafkaError._NO_OFFSET:
                    raise

    def _on_assign(self, consumer, partitions) -> None:
        self.aggregator.set_partitions(p.partition for p in partitions)
        log.info("Assigned partitions %s", sorted(p.partition for p in partitions))

    def run(self, idle_timeout: Optional[float] = None, max_messages: Optional[int] = None) -> dict:
        self.consumer.subscribe([self.settings.events_topic], on_assign=self._on_assign)
        last_message = time.monotonic()
        try:
            while self._running:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    if idle_timeout and time.monotonic() - last_message > idle_timeout:
                        log.info("No messages for %ss, stopping", idle_timeout)
                        break
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("Kafka error: %s", msg.error())
                    continue

                last_message = time.monotonic()
                self.metrics["consumed"] += 1
                try:
                    event = parse_event(msg.value())
                except InvalidEvent as exc:
                    self._dead_letter(msg.value(), str(exc))
                else:
                    if self.aggregator.add(event, msg.partition()):
                        self.metrics["valid"] += 1

                if self.metrics["consumed"] % 500 == 0:
                    self._flush()
                if max_messages and self.metrics["consumed"] >= max_messages:
                    break
            self._flush(final=True)
        finally:
            self.dlq.flush(10)
            self.consumer.close()
        self.metrics["late_events"] = self.aggregator.late_events
        log.info("Finished: %s", self.metrics)
        return self.metrics


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Windowed Kafka consumer")
    p.add_argument("--idle-timeout", type=float, help="exit after N seconds without messages")
    p.add_argument("--max-messages", type=int)
    a = p.parse_args()

    settings = Settings()
    sink = PostgresSink(settings.postgres_dsn)
    consumer = WindowedConsumer(settings, sink)
    signal.signal(signal.SIGINT, consumer.stop)
    signal.signal(signal.SIGTERM, consumer.stop)
    try:
        consumer.run(a.idle_timeout, a.max_messages)
    finally:
        sink.close()


if __name__ == "__main__":
    main()
