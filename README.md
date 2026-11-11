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

```
src/
├── common/    logging, metrics, Kafka producer/consumer
└── opensky/   stream.py, producer.py, consumer.py, storage.py
```

## Timing

- Producer polls the API every 0–10 minutes (random), about 288 requests/day, under OpenSky's anonymous limit of about 400/day.
- Consumer writes to Postgres every 60 seconds.
