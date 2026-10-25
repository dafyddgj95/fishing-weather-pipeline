import subprocess
import sys
from pathlib import Path

import dagster as dg
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


refresh_job = dg.define_asset_job(
    name="refresh_fishing_conditions",
    selection=dg.AssetSelection.all(),
)

every_six_hours = dg.ScheduleDefinition(
    job=refresh_job,
    cron_schedule="0 */6 * * *",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[raw_weather_forecast, fishing_dbt_assets],
    jobs=[refresh_job],
    schedules=[every_six_hours],
    resources={
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            profiles_dir=PROJECT_ROOT / "dbt",
        )
    },
)
