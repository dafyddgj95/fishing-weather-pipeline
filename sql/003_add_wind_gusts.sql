ALTER TABLE raw.weather_forecast
    ADD COLUMN IF NOT EXISTS wind_gusts_10m DOUBLE PRECISION;
