"""Kafka implementation of Publisher and Subscriber (confluent-kafka)."""

import json
from collections.abc import Callable, Iterator
from confluent_kafka import Consumer, Producer
from core.interfaces import Event


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.topic = topic
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})

    def publish(self, event: Event, on_result: Callable[[Exception | None], None]) -> None:
        def delivery(err, msg) -> None:
            on_result(Exception(str(err)) if err is not None else None)

        self.producer.produce(self.topic, value=json.dumps(event).encode("utf-8"), callback=delivery)
        self.producer.poll(0)  # trigger delivery callbacks


class KafkaSubscriber:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str) -> None:
        self.topic = topic
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        })

    def messages(self, on_error: Callable[[Exception], None]) -> Iterator[Event | None]:
        self.consumer.subscribe([self.topic])
        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                yield None
            elif msg.error():
                on_error(Exception(str(msg.error())))
            else:
                yield json.loads(msg.value().decode("utf-8"))
