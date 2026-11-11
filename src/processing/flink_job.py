"""
Flink (PyFlink) skeleton.

Planned flow: read BROKER topic -> parse JSON -> transform/aggregate -> write to STORE.
Not implemented yet. Add the code inside `run()`.
"""

import config


class FlinkProcessor:
    def __init__(self) -> None:
        self.bootstrap_servers = config.BOOTSTRAP_SERVERS
        self.topic = config.TOPIC
        self.database_url = config.DATABASE_URL

    def run(self) -> None:
        # TODO: create a StreamExecutionEnvironment
        # TODO: add a Kafka source (self.bootstrap_servers, self.topic)
        # TODO: parse the JSON value, transform / aggregate
        # TODO: add a sink to Postgres (self.database_url) or another store
        raise NotImplementedError("Flink job is not implemented yet")
