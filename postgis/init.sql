CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS gps_points (
    id SERIAL PRIMARY KEY,
    vehicle_id TEXT,
    event_time TIMESTAMP,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    speed DOUBLE PRECISION,
    district_name TEXT,
    geom geometry(Point, 4326)
);