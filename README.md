# Simple Streaming

Live flight data pipeline:
**OpenSky API → producer → Kafka → consumer → Postgres → Grafana**

## Run

```
make up
```

Then open Grafana at `localhost:3000`. Stop with `make down`.

Other commands: `make logs s=producer`, `make restart s=producer`, `make rebuild s=producer`, `make help`.

## Services

| Service | What it does |
|---|---|
| `kafka` | Single Kafka broker. |
| `kafka-init` | Creates the `opensky-topic` topic, then exits. |
| `producer` | Polls the OpenSky API and sends aircraft states to Kafka. |
| `consumer` | Reads from Kafka and saves to Postgres. |
| `postgres` | Stores the data. |
| `prometheus` | Collects producer and consumer metrics. |
| `grafana` | Dashboards and alerts. |

## Data

[OpenSky](https://openskynetwork.github.io/opensky-api/rest.html) is a free public API with the live state of aircraft (ID, callsign, country, position, altitude, speed).

Tables:
- `flight_states_raw` — every observation as received.
- `aircraft_current_state` — most recent state per aircraft.
- `country_stats` — running totals per country.

## Code

Each stage of the pipeline is a swappable module:

```
Source ─► Publisher ══ broker ══► Subscriber ─► Store
(ingestion)   (messaging)          (messaging)    (storage)
```

```
src/
├── core/interfaces.py   the contract for each stage
├── ingestion/           data sources     (opensky.py)
├── messaging/           brokers          (kafka.py)
├── storage/             destinations     (postgres.py)
├── processing/          engines          (spark_job.py, flink_job.py - skeletons)
├── config.py            picks which technology to use for each stage
├── producer.py          runs Source → Publisher
├── consumer.py          runs Subscriber → Store
├── processor.py         runs a processing engine (PROCESSOR=spark or flink)
└── common/              logging, metrics
```

Choose technologies with environment variables (defaults shown):
`SOURCE=opensky`, `BROKER=kafka`, `STORE=postgres`.

### Adding a new technology

1. Write a class that implements the matching interface in `core/interfaces.py`.
2. Register it in the matching dictionary in `config.py`.
3. Set the environment variable to its name.

Examples of where future work fits:
- **Another API:** add a file in `ingestion/`, register it under `SOURCES`.
- **Another broker:** add a file in `messaging/` with a publisher and subscriber, register under `PUBLISHERS` and `SUBSCRIBERS`.
- **PySpark / Flink:** they replace the consumer stage (read the topic, process, write to a store). Skeletons are ready in `processing/`: fill in `run()` in `spark_job.py` or `flink_job.py`, uncomment the `processor` service in `docker-compose.yml`, and set `PROCESSOR`. The producer, Kafka and Postgres stay unchanged.

## Timing

- Producer polls the API once every 15 minutes (about 96 requests/day), well under OpenSky's anonymous limit of about 400/day.
- Consumer writes to Postgres every 60 seconds.
