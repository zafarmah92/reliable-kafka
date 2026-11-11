-- Flight data schema (OpenSky). Runs automatically on first Postgres start.

-- Append-only raw log: every observation ever seen, never overwritten.
CREATE TABLE IF NOT EXISTS flight_states_raw (
    icao24 TEXT,
    callsign TEXT,
    origin_country TEXT,
    longitude REAL,
    latitude REAL,
    baro_altitude REAL,
    on_ground BOOLEAN,
    velocity REAL,
    true_track REAL,
    vertical_rate REAL,
    last_contact BIGINT
);

-- Speeds up the time-bucketed dashboard query, which filters/groups by
-- last_contact over an ever-growing, unbounded table.
CREATE INDEX IF NOT EXISTS idx_flight_states_raw_last_contact ON flight_states_raw (last_contact);

-- CDC-style table: one row per aircraft (icao24), upserted every batch so
-- it always reflects the latest known state, not a history of changes.
CREATE TABLE IF NOT EXISTS aircraft_current_state (
    icao24 TEXT PRIMARY KEY,
    callsign TEXT,
    origin_country TEXT,
    longitude REAL,
    latitude REAL,
    baro_altitude REAL,
    on_ground BOOLEAN,
    velocity REAL,
    true_track REAL,
    vertical_rate REAL,
    last_contact BIGINT
);

-- Aggregated table: one row per origin_country, merged every batch by
-- adding that batch's counts/sums onto the running totals.
CREATE TABLE IF NOT EXISTS country_stats (
    origin_country TEXT PRIMARY KEY,
    observation_count BIGINT,
    velocity_sum DOUBLE PRECISION,
    velocity_count BIGINT,
    altitude_sum DOUBLE PRECISION,
    altitude_count BIGINT
);
