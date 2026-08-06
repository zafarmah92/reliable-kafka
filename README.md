# Simple Streaming

The project has two independent pipelines that both
follow the same pattern: **producer → Kafka → consumer → Postgres → Grafana**.

## Running the project (Makefile)

The `Makefile` wraps the common Docker Compose commands, so you don't have
to remember them.

- `make up` — build and start the whole stack.
- `make down` — stop everything.
- `make restart s=producer` — restart one service (leave `s` empty for all).
- `make rebuild s=producer` — rebuild the image and recreate one service.
- `make logs s=producer` — follow logs for one service (leave `s` empty for all).
- `make venv` — create a local Python virtualenv with `uv`, so your editor can resolve imports. Not needed to actually run the project — each container installs its own dependencies.
- `make help` — list all available commands.

To run the project: `make up`, then open Grafana at `localhost:3000`.

## Docker Compose services

| Service | What it does |
|---|---|
| `kafka` | A standalone kafka broker |
| `kafka-init` | Runs once at startup. Creates the two Kafka topics. |
| `producer` | Sends MovieLens movie ratings to Kafka. |
| `consumer` | Reads MovieLens ratings from Kafka and saves them to Postgres. |
| `opensky-producer` | Sends live flight data from the OpenSky API to Kafka. |
| `opensky-consumer` | Reads flight data from Kafka and saves it to Postgres. |
| `postgres` | Stores all the data, in two databases: `movielens` and `opensky`. |
| `prometheus` | Collects metrics from the producers and consumers (messages sent, received, errors). |
| `grafana` | Shows dashboards: SLO style pipeline alerts and the KPI boards for each dataset. |

`opensky-producer` and `opensky-consumer` are currently commented out in
`docker-compose.yml`. 

## Kafka

Kafka runs as a single broker. Main settings:

- `KAFKA_PROCESS_ROLES: controller,broker` — one node acts as both.
- `KAFKA_LISTENERS` / `KAFKA_ADVERTISED_LISTENERS` — clients connect to it (`kafka:9092`).
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE: true` — safety net, though we create topics explicitly.

**`kafka-init`** exists so the topics are ready *before* any producer or
consumer tries to use them. It runs once, creates the topics, then exits.

### Topics

- **`test-topic`** — used to simulate the well-known **MovieLens** dataset
  (movie ratings).
- **`opensky-topic`** — a step further: live data pulled from the **OpenSky
  Network** public API.

## What is OpenSky?

OpenSky Network is a free, public API that tracks real aircraft in the sky
right now. Each record is one aircraft's current state: its ID (`icao24`),
callsign, country, position (latitude/longitude), altitude, speed, and
whether it's on the ground or flying.

Reference: https://openskynetwork.github.io/opensky-api/rest.html

## Database schema

### MovieLens (`movielens` database)

- `movie_ratings` — raw table. Every rating event, exactly as it arrived.
- `movie_stats` — one row per movie: average rating and rating count.
- `rating_distribution` — one row per rating value (1, 2, ...): how many times it was given.
- `user_activity` — one row per user: how many ratings they made.

### OpenSky (`opensky` database)

- `flight_states_raw` — raw table. Every aircraft observation, exactly as it arrived.
- `aircraft_current_state` — one row per aircraft: its latest known state.
- `country_stats` — one row per country: how many observations, and running totals for speed/altitude.

## Code structure

The code lives under `src/`, split into two modules that mirror each other:

```
src/
├── common/         shared code (logging, Prometheus metrics)
├── movielens/       producer.py, consumer.py, storage.py, stream.py
└── opensky/         producer.py, consumer.py, storage.py, stream.py
```

Both modules use the same four file names, with the same job in each:

- **`stream.py`** — fetches the raw data (reads the MovieLens CSV, or polls the OpenSky API).
- **`producer.py`** — takes each record from `stream.py` and sends it to Kafka.
- **`consumer.py`** — reads records from Kafka and hands them to `storage.py`.
- **`storage.py`** — the most important part. Decides how data is saved to Postgres.

## Timing: how often producers and consumers run

- **MovieLens producer** — sends one rating at a random interval between 0 and 30 seconds.
- **MovieLens consumer** — flushes buffered ratings to Postgres every 30 seconds.
- **OpenSky producer** — polls the OpenSky API at a random interval between 0 and 10 minutes (600 seconds).
- **OpenSky consumer** — flushes buffered aircraft states to Postgres every 60 seconds.

Producers use a *random* delay so sending isn't perfectly steady. Consumers
use a fixed flush interval — they buffer everything they receive and write
it to Postgres in one batch when the interval is reached.

### OpenSky API request limit

OpenSky's public API only allows about **400 requests per day** for
anonymous (non-authenticated) users. The OpenSky producer's 0–10 minute
polling interval averages around 5 minutes between requests, which is
~288 requests/day — safely under that limit. Polling faster than this
risks `429 Too Many Requests` errors from the API.

## Storage — how ingestion works

Both consumers buffer incoming messages in memory and write to Postgres in
batches (a "flush"), instead of one row at a time. TO avoid intensive parallel write, which can lead to locking the tables.

### MovieLens: aggregation

Every flush does two things:

1. Insert the raw batch into `movie_ratings`.
2. Aggregate that same batch in Python (count and average per movie, count per rating value, count per user), then **merge** those numbers into `movie_stats`, `rating_distribution`, and `user_activity`.

"Merge" means add-on-top, not overwrite: each flush's numbers are added to
whatever is already in the table (`ON CONFLICT ... DO UPDATE SET count =
count + new_count`). So the aggregate tables always hold a running total
across all batches ever processed, without needing to re-scan the raw
table.

### OpenSky: aggregation + CDC

Every flush does three things:

1. Insert the raw batch into `flight_states_raw`, as-is (a growing history log).
2. **CDC-style upsert** into `aircraft_current_state`, keyed by aircraft ID (`icao24`). Each flush overwrites that aircraft's row with its latest position/speed/altitude. So this table is never a history — it's always just "where is every aircraft right now."
3. Aggregate the batch by country (count, speed/altitude totals) and merge it into `country_stats`, the same add-on-top way MovieLens does.

So OpenSky combines both ideas: a raw history log, a CDC table for current
state, and a merged aggregate table — all from the same batch.
