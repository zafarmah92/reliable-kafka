"""
###################################################
Very simple Kafka producer using confluent-kafka.
Streams live OpenSky Network aircraft states onto the topic, forever.
###################################################
"""

import os
import sys
from common.logger import get_logger
from common.kafka_producer import KafkaProducerService
from opensky.stream import OpenSkyStream

TOPIC = "opensky-topic"
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
METRICS_PORT = 8000

logger = get_logger(__name__)


if __name__ == "__main__":
    service = KafkaProducerService(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=TOPIC,
        metrics_port=METRICS_PORT,
        stream=OpenSkyStream(),
        logger=logger,
    )
    try:
        service.run()
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    except Exception:
        logger.exception("Producer crashed with an unhandled exception")
        sys.exit(1)
