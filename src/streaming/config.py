"""Runtime settings, overridable with environment variables."""

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = field(
        default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    events_topic: str = field(default_factory=lambda: _env("EVENTS_TOPIC", "ecommerce.events"))
    dlq_topic: str = field(default_factory=lambda: _env("DLQ_TOPIC", "ecommerce.events.dlq"))
    consumer_group: str = field(default_factory=lambda: _env("CONSUMER_GROUP", "window-aggregator"))
    postgres_dsn: str = field(
        default_factory=lambda: _env(
            "POSTGRES_DSN", "postgresql://streaming:streaming@localhost:5432/streaming"
        )
    )
    window_seconds: int = field(default_factory=lambda: int(_env("WINDOW_SECONDS", "60")))
    allowed_lateness_seconds: int = field(
        default_factory=lambda: int(_env("ALLOWED_LATENESS_SECONDS", "30"))
    )
