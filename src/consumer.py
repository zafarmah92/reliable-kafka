"""Storage stage: Subscriber -> Store. Pick technologies in config.py."""

import sys
import config
from common.logger import get_logger
from common.metrics import GrafanaMetrics

logger = get_logger("consumer")


def main() -> None:
    metrics = GrafanaMetrics(config.METRICS_PORT)
    metrics.start()
    subscriber = config.build_subscriber()
    store = config.build_store()

    def on_error(err: Exception) -> None:
        metrics.record_consumer_error()
        logger.error(f"Consumer error: {err}")

    logger.info(f"Consumer started: broker={config.BROKER}, store={config.STORE}")
    for event in subscriber.messages(on_error):
        if event is None:
            store.maybe_flush()
            continue
        metrics.record_received()
        store.add(event)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    except Exception:
        logger.exception("Consumer crashed with an unhandled exception")
        sys.exit(1)
