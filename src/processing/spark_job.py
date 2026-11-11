"""
PySpark (Structured Streaming) skeleton.

Planned flow: read BROKER topic -> parse JSON -> transform/aggregate -> write to STORE.
Not implemented yet. Add the code inside `run()`.
"""

import config


class SparkProcessor:
    def __init__(self) -> None:
        self.bootstrap_servers = config.BOOTSTRAP_SERVERS
        self.topic = config.TOPIC
        self.database_url = config.DATABASE_URL

    def run(self) -> None:
        # TODO: build a SparkSession
        # TODO: readStream from Kafka (self.bootstrap_servers, self.topic)
        # TODO: parse the JSON value using the flight schema
        # TODO: transform / aggregate
        # TODO: writeStream to Postgres (self.database_url) or another sink
        raise NotImplementedError("PySpark job is not implemented yet")
