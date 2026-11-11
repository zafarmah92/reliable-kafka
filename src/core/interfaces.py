"""
Contracts for each stage of the pipeline. Every technology (Kafka, Spark,
Flink, Postgres, ...) plugs in by implementing one of these, so the stages
can be swapped without touching the others.

    Source -> Publisher ==(broker)==> Subscriber -> Store
"""

from collections.abc import Callable, Iterator
from typing import Any, Protocol

Event = dict[str, Any]


class Source(Protocol):
    """Ingestion: where data comes from (an API, a file, ...)."""

    def events(self) -> Iterator[Event]:
        """Yield events forever."""
        ...


class Publisher(Protocol):
    """Messaging, write side: puts events onto a broker/topic."""

    def publish(self, event: Event, on_result: Callable[[Exception | None], None]) -> None:
        """Send one event. `on_result` is called with None on success, or the error."""
        ...


class Subscriber(Protocol):
    """Messaging, read side: reads events back from the broker/topic."""

    def messages(self, on_error: Callable[[Exception], None]) -> Iterator[Event | None]:
        """Yield events forever. Yield None when idle so callers can run timers."""
        ...


class Store(Protocol):
    """Storage: where processed data ends up (Postgres, a lake, ...)."""

    def add(self, event: Event) -> None: ...

    def maybe_flush(self) -> None: ...


class Processor(Protocol):
    """Processing: a long-running job that reads the topic, transforms the
    data and writes the result to a store (PySpark, Flink, ...). It replaces
    the plain consumer stage when you want an engine to do the work."""

    def run(self) -> None: ...
