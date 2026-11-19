CREATE TABLE IF NOT EXISTS ride_events (
    event_id TEXT PRIMARY KEY,
    ride_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    customer_id TEXT,
    driver_id TEXT,
    city TEXT NOT NULL,
    zone TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    dropoff_latitude DOUBLE PRECISION,
    dropoff_longitude DOUBLE PRECISION,
    event_timestamp TIMESTAMPTZ NOT NULL,
    kafka_timestamp TIMESTAMPTZ,
    source_topic TEXT,
    source_partition INTEGER,
    source_offset BIGINT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ride_events_ride_id
ON ride_events(ride_id);

CREATE INDEX IF NOT EXISTS idx_ride_events_event_timestamp
ON ride_events(event_timestamp);

CREATE INDEX IF NOT EXISTS idx_ride_events_zone
ON ride_events(zone);


-- Temporary staging table for idempotent batch writes

CREATE TABLE IF NOT EXISTS ride_events_staging (
    event_id TEXT,
    ride_id TEXT,
    event_type TEXT,
    customer_id TEXT,
    driver_id TEXT,
    city TEXT,
    zone TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    dropoff_latitude DOUBLE PRECISION,
    dropoff_longitude DOUBLE PRECISION,
    event_timestamp TIMESTAMPTZ,
    kafka_timestamp TIMESTAMPTZ,
    source_topic TEXT,
    source_partition INTEGER,
    source_offset BIGINT,
    batch_id BIGINT,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_staging_batch_id
ON ride_events_staging(batch_id);