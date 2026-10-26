import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import dagster as dg
import psycopg
import requests
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

dbt_project = DbtProject(project_dir=PROJECT_ROOT / "dbt")
dbt_project.prepare_if_dev()


@dg.asset(
    key=["raw", "weather_forecast"],
    group_name="ingestion",
    description="Hourly forecasts for both lakes, fetched from Open-Meteo.",
)
def raw_weather_forecast(context: dg.AssetExecutionContext) -> None:
    result = subprocess.run(
        [sys.executable, "src/ingest_weather.py"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    context.log.info(result.stderr.strip() or result.stdout.strip())
    if result.returncode != 0:
        raise Exception("Ingestion script failed; see the log above")


@dbt_assets(manifest=dbt_project.manifest_path)
def fishing_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@dg.asset(
    deps=["fct_daily_summary"],
    group_name="alerts",
    description="Daily fishing summary pushed to the phone via ntfy.",
)
def daily_fishing_alert(context: dg.AssetExecutionContext) -> None:
    with psycopg.connect(
        host="127.0.0.1",
        port=5432,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    ) as conn:
        rows = conn.execute(
            """
            select lake, local_date, best_window_start, best_window_score, best_window_rating
            from analytics.fct_daily_summary
            where local_date >= (now() at time zone 'Europe/London')::date
              and local_date < (now() at time zone 'Europe/London')::date + 3
            order by lake, local_date
            """
        ).fetchall()

    london = ZoneInfo("Europe/London")
    lines = []
    current_lake = None
    for lake, day, start, score, rating in rows:
        if lake != current_lake:
            if current_lake is not None:
                lines.append("")
            lines.append(lake)
            current_lake = lake
        if start is None:
            lines.append(f"{day:%a %d %b}: not enough daylight to rate")
        else:
            begin = start.astimezone(london)
            end = begin + timedelta(hours=3)
            lines.append(
                f"{day:%a %d %b}: {rating} ({score}), best {begin:%H:%M}-{end:%H:%M}"
            )

    message = "\n".join(lines) or "No in-season fishing days in the next 3 days."
    context.log.info(message)

    response = requests.post(
        f"https://ntfy.sh/{os.environ['NTFY_TOPIC']}",
        data=message.encode("utf-8"),
        headers={"Title": "Fishing forecast", "Tags": "fishing_pole_and_fish"},
        timeout=30,
    )
    response.raise_for_status()


refresh_job = dg.define_asset_job(
    name="refresh_fishing_conditions",
    selection=dg.AssetSelection.all() - dg.AssetSelection.assets("daily_fishing_alert"),
)

alert_job = dg.define_asset_job(
    name="send_daily_alert",
    selection=dg.AssetSelection.assets("daily_fishing_alert"),
)

every_six_hours = dg.ScheduleDefinition(
    job=refresh_job,
    cron_schedule="0 */6 * * *",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

daily_alert_schedule = dg.ScheduleDefinition(
    job=alert_job,
    cron_schedule="30 7 * * *",
    execution_timezone="Europe/London",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[raw_weather_forecast, fishing_dbt_assets, daily_fishing_alert],
    jobs=[refresh_job, alert_job],
    schedules=[every_six_hours, daily_alert_schedule],
    resources={
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            profiles_dir=PROJECT_ROOT / "dbt",
        )
    },
)
