"""
######################################################
Single shared Prometheus metrics class used by both the producer and the
consumer. Grafana reads these counters through the Prometheus datasource.
######################################################
"""

from prometheus_client import Counter, start_http_server
from common.logger import get_logger

logger = get_logger(__name__)


class GrafanaMetrics:
    def __init__(self, port: int) -> None:
        self.port: int = port
        self.messages_produced: Counter = Counter(
            "messages_produced_total", "Total number of messages successfully delivered to Kafka"
        )
        self.messages_produce_failed: Counter = Counter(
            "messages_produce_failed_total", "Total number of messages that failed to be delivered"
        )
        self.messages_received: Counter = Counter(
            "messages_received_total", "Total number of messages received from Kafka"
        )
        self.consumer_errors: Counter = Counter(
            "consumer_errors_total", "Total number of consumer poll errors"
        )

    def start(self) -> None:
        start_http_server(self.port)
        logger.info(f"Metrics server listening on :{self.port}/metrics")

    # producer-side
    def record_produced(self) -> None:
        self.messages_produced.inc()

    def record_produce_failed(self) -> None:
        self.messages_produce_failed.inc()

    # consumer-side
    def record_received(self) -> None:
        self.messages_received.inc()

    def record_consumer_error(self) -> None:
        self.consumer_errors.inc()
