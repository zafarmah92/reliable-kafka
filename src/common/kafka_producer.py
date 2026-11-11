"""
###################################################
Shared producer loop used by both pipelines: connects to Kafka, sends
every item from a stream, and reports delivery success/failure.
###################################################
"""

import json
from collections.abc import Callable
from logging import Logger
from typing import Any
from confluent_kafka import Producer, KafkaError, Message
from common.metrics import GrafanaMetrics


class KafkaProducerService:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        metrics_port: int,
        stream: Any,
        logger: Logger,
        to_message: Callable[[Any], dict[str, Any]] = lambda item: item,
    ) -> None:
        self.topic: str = topic
        self.producer: Producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.metrics: GrafanaMetrics = GrafanaMetrics(metrics_port)
        self.stream = stream
        self.logger: Logger = logger
        self.to_message = to_message

    def delivery_report(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.metrics.record_produce_failed()
            self.logger.error(f"Delivery failed: {err}")
        else:
            self.metrics.record_produced()
            self.logger.info(f"Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def run(self) -> None:
        self.metrics.start()
        self.logger.info(f"Producer connected, streaming to topic '{self.topic}'")

        for item in self.stream.events():
            message = self.to_message(item)
            try:
                self.producer.produce(
                    self.topic,
                    value=json.dumps(message).encode("utf-8"),
                    callback=self.delivery_report,
                )
                self.producer.poll(0)  # trigger delivery callbacks
                self.logger.info(f"Sent: {message}")
            except Exception:
                self.logger.exception("Error sending message")
