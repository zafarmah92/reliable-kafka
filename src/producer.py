"""Ingestion stage: Source -> Publisher. Pick technologies in config.py."""

import sys
import config
from common.logger import get_logger
from common.metrics import GrafanaMetrics

logger = get_logger("producer")


def main() -> None:
    metrics = GrafanaMetrics(config.METRICS_PORT)
    metrics.start()
    source = config.build_source()
    publisher = config.build_publisher()

    def on_result(err: Exception | None) -> None:
        if err is None:
            metrics.record_produced()
        else:
            metrics.record_produce_failed()
            logger.error(f"Delivery failed: {err}")

    logger.info(f"Producer started: source={config.SOURCE}, broker={config.BROKER}")
    for event in source.events():
        try:
            publisher.publish(event, on_result)
        except Exception:
            metrics.record_produce_failed()
            logger.exception("Error sending message")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    except Exception:
        logger.exception("Producer crashed with an unhandled exception")
        sys.exit(1)
