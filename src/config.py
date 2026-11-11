"""
All settings and the "which technology?" choices live here. Each stage is
picked by an environment variable and built by a small factory, so adding a
new technology = write one class + add one line to a registry below.
"""

import os
from core.interfaces import Processor, Publisher, Source, Store, Subscriber

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/opensky")
TOPIC = os.environ.get("TOPIC", "opensky-topic")
GROUP_ID = os.environ.get("GROUP_ID", "opensky-consumer-group")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))

SOURCE = os.environ.get("SOURCE", "opensky")
BROKER = os.environ.get("BROKER", "kafka")
STORE = os.environ.get("STORE", "postgres")
PROCESSOR = os.environ.get("PROCESSOR", "spark")  # only used by processor.py


def _opensky_source() -> Source:
    from ingestion.opensky import OpenSkySource
    return OpenSkySource()


def _kafka_publisher() -> Publisher:
    from messaging.kafka import KafkaPublisher
    return KafkaPublisher(BOOTSTRAP_SERVERS, TOPIC)


def _kafka_subscriber() -> Subscriber:
    from messaging.kafka import KafkaSubscriber
    return KafkaSubscriber(BOOTSTRAP_SERVERS, TOPIC, GROUP_ID)


def _postgres_store() -> Store:
    from storage.postgres import PostgresFlightStore
    return PostgresFlightStore(DATABASE_URL)


def _spark_processor() -> Processor:
    from processing.spark_job import SparkProcessor
    return SparkProcessor()


def _flink_processor() -> Processor:
    from processing.flink_job import FlinkProcessor
    return FlinkProcessor()


# ---- registries: add new technologies here ----
SOURCES = {"opensky": _opensky_source}
PUBLISHERS = {"kafka": _kafka_publisher}
SUBSCRIBERS = {"kafka": _kafka_subscriber}
STORES = {"postgres": _postgres_store}
PROCESSORS = {"spark": _spark_processor, "flink": _flink_processor}


def _pick(registry: dict, name: str, kind: str):
    if name not in registry:
        raise ValueError(f"Unknown {kind} '{name}'. Available: {', '.join(registry)}")
    return registry[name]()


def build_source() -> Source:
    return _pick(SOURCES, SOURCE, "SOURCE")


def build_publisher() -> Publisher:
    return _pick(PUBLISHERS, BROKER, "BROKER")


def build_subscriber() -> Subscriber:
    return _pick(SUBSCRIBERS, BROKER, "BROKER")


def build_store() -> Store:
    return _pick(STORES, STORE, "STORE")


def build_processor() -> Processor:
    return _pick(PROCESSORS, PROCESSOR, "PROCESSOR")
