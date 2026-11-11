"""
###################################################
Buffers OpenSky events in memory, then saves them to Postgres in batches -
the raw log, plus a table with each aircraft's latest known state.
###################################################
"""

import time
from collections import defaultdict
from typing import Any
import psycopg2
from psycopg2.extensions import connection as Connection
from psycopg2.extras import execute_values
from common.logger import get_logger

logger = get_logger(__name__)

###########################################
# Add every new aircraft observation from this batch as a new row.
# Nothing is ever updated or replaced here - this table just keeps growing.
###########################################
INSERT_RAW_SQL = """
    INSERT INTO flight_states_raw
        (icao24, callsign, origin_country, longitude, latitude, baro_altitude,
         on_ground, velocity, true_track, vertical_rate, last_contact)
    VALUES %s
"""

###########################################
# For each aircraft (icao24): if it's new, add its row. If it already
# has a row, overwrite it with the latest position/speed/altitude - but
# only if this new data is not older than what's already stored.
###########################################
UPSERT_CURRENT_STATE_SQL = """
    INSERT INTO aircraft_current_state
        (icao24, callsign, origin_country, longitude, latitude, baro_altitude,
         on_ground, velocity, true_track, vertical_rate, last_contact)
    VALUES %s
    ON CONFLICT (icao24) DO UPDATE SET
        callsign = EXCLUDED.callsign,
        origin_country = EXCLUDED.origin_country,
        longitude = EXCLUDED.longitude,
        latitude = EXCLUDED.latitude,
        baro_altitude = EXCLUDED.baro_altitude,
        on_ground = EXCLUDED.on_ground,
        velocity = EXCLUDED.velocity,
        true_track = EXCLUDED.true_track,
        vertical_rate = EXCLUDED.vertical_rate,
        last_contact = EXCLUDED.last_contact
    WHERE EXCLUDED.last_contact >= aircraft_current_state.last_contact
"""

###########################################
# For each country: if it's new, add its row. If it already has a row,
# just add this batch's counts and totals on top of the old ones.
###########################################
UPSERT_COUNTRY_STATS_SQL = """
    INSERT INTO country_stats
        (origin_country, observation_count, velocity_sum, velocity_count, altitude_sum, altitude_count)
    VALUES %s
    ON CONFLICT (origin_country) DO UPDATE SET
        observation_count = country_stats.observation_count + EXCLUDED.observation_count,
        velocity_sum = country_stats.velocity_sum + EXCLUDED.velocity_sum,
        velocity_count = country_stats.velocity_count + EXCLUDED.velocity_count,
        altitude_sum = country_stats.altitude_sum + EXCLUDED.altitude_sum,
        altitude_count = country_stats.altitude_count + EXCLUDED.altitude_count
"""


def country_stats_from_batch(batch: list[tuple]) -> list[tuple]:
    # --- add up counts/speed/altitude per country; skip missing values instead of counting them as zero ---
    totals: dict[str, dict[str, float]] = defaultdict(
        lambda: {"count": 0, "velocity_sum": 0.0, "velocity_count": 0, "altitude_sum": 0.0, "altitude_count": 0}
    )
    for _, _, origin_country, _, _, baro_altitude, _, velocity, _, _, _ in batch:
        if origin_country is None:
            continue
        entry = totals[origin_country]
        entry["count"] += 1
        if velocity is not None:
            entry["velocity_sum"] += velocity
            entry["velocity_count"] += 1
        if baro_altitude is not None:
            entry["altitude_sum"] += baro_altitude
            entry["altitude_count"] += 1
    return [
        (country, t["count"], t["velocity_sum"], t["velocity_count"], t["altitude_sum"], t["altitude_count"])
        for country, t in totals.items()
    ]


class OpenSkyEventStore:
    def __init__(self, dsn: str, flush_interval_seconds: int = 60) -> None:
        self.conn: Connection = psycopg2.connect(dsn)
        self.conn.autocommit = True
        self.flush_interval_seconds: int = flush_interval_seconds
        self.buffer: list[tuple] = []
        self.last_flush: float = time.time()

    def add(self, event: dict[str, Any]) -> None:
        self.buffer.append((
            event.get("icao24"),
            event.get("callsign"),
            event.get("origin_country"),
            event.get("longitude"),
            event.get("latitude"),
            event.get("baro_altitude"),
            event.get("on_ground"),
            event.get("velocity"),
            event.get("true_track"),
            event.get("vertical_rate"),
            event.get("last_contact"),
        ))
        self.maybe_flush()

    def maybe_flush(self) -> None:
        if self.buffer and time.time() - self.last_flush >= self.flush_interval_seconds:
            self.flush()

    def flush(self) -> None:
        with self.conn.cursor() as cur:
            execute_values(cur, INSERT_RAW_SQL, self.buffer)
            execute_values(cur, UPSERT_CURRENT_STATE_SQL, self.buffer)
            execute_values(cur, UPSERT_COUNTRY_STATS_SQL, country_stats_from_batch(self.buffer))
        logger.info(f"Flushed {len(self.buffer)} raw states, upserted current-state (CDC), and merged country_stats")
        self.buffer.clear()
        self.last_flush = time.time()
