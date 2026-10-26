import logging
import os
import time
from datetime import datetime, timezone

import psycopg
import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

LAKES = {
    "Llyn Brenig": (53.07, -3.51),
    "Chew Valley Lake": (51.33, -2.62),
}
API_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARS = [
    "temperature_2m", "pressure_msl", "wind_speed_10m",
    "wind_direction_10m", "cloud_cover", "precipitation", "is_day", "wind_gusts_10m",
]
INSERT_SQL = """
    INSERT INTO raw.weather_forecast
        (lake, forecast_time, fetched_at, temperature_2m, pressure_msl,
         wind_speed_10m, wind_direction_10m, cloud_cover, precipitation, is_day, wind_gusts_10m)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
"""


def fetch_hourly(lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_VARS),
        "wind_speed_unit": "mph",
        "timezone": "UTC",
        "past_days": 7,
        "forecast_days": 3,
    }
    for attempt in range(1, 4):
        try:
            response = requests.get(API_URL, params=params, timeout=30)
            response.raise_for_status()
            return response.json()["hourly"]
        except requests.RequestException as exc:
            logging.warning("Attempt %s failed: %s", attempt, exc)
            if attempt == 3:
                raise
            time.sleep(5 * attempt)


def build_rows(lake, hourly, fetched_at):
    rows = []
    for i, time_text in enumerate(hourly["time"]):
        forecast_time = datetime.fromisoformat(time_text).replace(tzinfo=timezone.utc)
        values = [hourly[var][i] for var in HOURLY_VARS]
        rows.append((lake, forecast_time, fetched_at, *values))
    return rows


def load_rows(rows):
    with psycopg.connect(
        host="127.0.0.1",
        port=5432,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    ) as conn:
        with conn.cursor() as cur:
            cur.executemany(INSERT_SQL, rows)


def main():
    fetched_at = datetime.now(timezone.utc)
    all_rows = []
    for lake, (lat, lon) in LAKES.items():
        hourly = fetch_hourly(lat, lon)
        rows = build_rows(lake, hourly, fetched_at)
        logging.info("Fetched %s rows for %s", len(rows), lake)
        all_rows.extend(rows)
    load_rows(all_rows)
    logging.info("Loaded %s rows into raw.weather_forecast", len(all_rows))


if __name__ == "__main__":
    main()
