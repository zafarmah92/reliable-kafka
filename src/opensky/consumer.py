"""
###################################################
Very simple Kafka consumer using confluent-kafka.
Reads and stores OpenSky aircraft-state messages from the topic, forever.
###################################################
"""

import os
import sys
from common.logger import get_logger
from common.kafka_consumer import KafkaConsumerService
from opensky.storage import OpenSkyEventStore

TOPIC = "opensky-topic"
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/opensky")
METRICS_PORT = 8001
GROUP_ID = "opensky-consumer-group"

logger = get_logger(__name__)


if __name__ == "__main__":
    service = KafkaConsumerService(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=TOPIC,
        group_id=GROUP_ID,
        metrics_port=METRICS_PORT,
        store=OpenSkyEventStore(DATABASE_URL),
        logger=logger,
    )
    try:
        service.run()
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    except Exception:
        logger.exception("Consumer crashed with an unhandled exception")
        sys.exit(1)
