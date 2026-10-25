ALTER TABLE raw.weather_forecast
    ADD COLUMN IF NOT EXISTS is_day SMALLINT;
