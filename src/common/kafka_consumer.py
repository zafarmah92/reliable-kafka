"""
###################################################
Shared consumer loop used by both pipelines: polls Kafka, hands each
message to a storage object, and flushes it on a timer.
###################################################
"""

import json
from logging import Logger
from typing import Any
from confluent_kafka import Consumer, Message
from common.metrics import GrafanaMetrics


class KafkaConsumerService:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        metrics_port: int,
        store: Any,
        logger: Logger,
    ) -> None:
        self.topic: str = topic
        self.consumer: Consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",  # read from the beginning if no offset stored
        })
        self.metrics: GrafanaMetrics = GrafanaMetrics(metrics_port)
        self.store = store
        self.logger: Logger = logger

    def run(self) -> None:
        self.metrics.start()
        self.consumer.subscribe([self.topic])
        self.logger.info(f"Consumer listening on topic '{self.topic}'...")

        while True:
            msg: Message | None = self.consumer.poll(timeout=1.0)
            if msg is None:
                self.store.maybe_flush()
                continue
            if msg.error():
                self.metrics.record_consumer_error()
                self.logger.error(f"Consumer error: {msg.error()}")
                continue

            value: dict[str, Any] = json.loads(msg.value().decode("utf-8"))
            self.metrics.record_received()
            self.store.add(value)
            self.logger.info(f"Received: {value} (partition={msg.partition()}, offset={msg.offset()})")
