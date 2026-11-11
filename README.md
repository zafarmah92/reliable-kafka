# Simple Streaming

A small streaming pipeline for live flight data:
**OpenSky API → producer → Kafka → consumer → Postgres → Grafana**.

## Running the project (Makefile)

- `make up` — build and start the whole stack.
- `make down` — stop everything.
- `make restart s=producer` — restart one service (leave `s` empty for all).
- `make rebuild s=producer` — rebuild the image and recreate one service.
- `make logs s=producer` — follow logs for one service (leave `s` empty for all).
- `make venv` — create a local Python virtualenv with `uv` so your editor can resolve imports. Not needed to run the project.
- `make help` — list all available commands.

To run the project: `make up`, then open Grafana at `localhost:3000`.

## Docker Compose services

| Service | What it does |
|---|---|
| `kafka` | A standalone Kafka broker. |
| `kafka-init` | Runs once at startup and creates the `opensky-topic` topic. |
| `producer` | Polls the OpenSky API and sends aircraft states to Kafka. |
| `consumer` | Reads aircraft states from Kafka and saves them to Postgres. |
| `postgres` | Stores the data (`opensky` database). |
| `prometheus` | Collects metrics from the producer and consumer (messages sent, received, errors). |
| `grafana` | Dashboards: pipeline SLOs and flight KPIs, plus alert rules. |

## What is OpenSky?

OpenSky Network is a free, public API that tracks real aircraft in the sky
right now. Each record is one aircraft's current state: its ID (`icao24`),
callsign, country, position, altitude, speed, and whether it's on the ground.

Reference: https://openskynetwork.github.io/opensky-api/rest.html

## Database schema

- `flight_states_raw` — every aircraft observation, exactly as it arrived.
- `aircraft_current_state` — one row per aircraft: its latest known state.
- `country_stats` — one row per country: observation count and running speed/altitude totals.

## Code structure

```
src/
├── common/    shared code (logging, Prometheus metrics, Kafka producer/consumer services)
└── opensky/   stream.py, producer.py, consumer.py, storage.py
```

- **`stream.py`** — polls the OpenSky API.
- **`producer.py`** — sends each record to Kafka.
- **`consumer.py`** — reads records from Kafka and hands them to `storage.py`.
- **`storage.py`** — decides how data is saved to Postgres.

## Timing

- **Producer** — polls the API at a random interval between 0 and 10 minutes.
- **Consumer** — buffers messages and flushes to Postgres every 60 seconds.

OpenSky's anonymous API allows about **400 requests per day**. The average
5-minute polling interval is ~288 requests/day, safely under that limit.

## Storage

Every flush does three things:

1. Insert the raw batch into `flight_states_raw` (a growing history log).
2. **CDC-style upsert** into `aircraft_current_state`, keyed by `icao24`, so it always shows where every aircraft is right now.
3. Aggregate the batch by country and **merge** it into `country_stats` by adding onto the running totals.
