CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.weather_forecast (
    lake                TEXT        NOT NULL,
    forecast_time       TIMESTAMPTZ NOT NULL,
    fetched_at          TIMESTAMPTZ NOT NULL,
    temperature_2m      DOUBLE PRECISION,
    pressure_msl        DOUBLE PRECISION,
    wind_speed_10m      DOUBLE PRECISION,
    wind_direction_10m  DOUBLE PRECISION,
    cloud_cover         DOUBLE PRECISION,
    precipitation       DOUBLE PRECISION,
    PRIMARY KEY (lake, forecast_time, fetched_at)
);
