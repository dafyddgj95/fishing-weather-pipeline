# Fly Fishing Conditions Pipeline

A data pipeline that collects weather data for two fly fishing lakes, Llyn Brenig (North Wales) and Chew Valley Lake (Somerset), and scores how good the conditions are for fly fishing.

## Roadmap

### Phase 1: Conditions pipeline
- Ingest hourly weather forecasts (Open-Meteo API) for both lakes, running every 6 hours
- Store raw and cleaned data in a database, keeping a timestamp of when each forecast was fetched
- Transform with dbt, orchestrate with Dagster
- Score each hour for fly fishing (wind, pressure trend, cloud cover, rain, temperature)
- Dashboard and daily summary alert

### Phase 2: Catch log app
- Anglers log catches: lake, date and time, fly pattern, size, notes
- Each catch is linked to the weather conditions at that time

### Phase 3: Fly suggestions
- Use the accumulated catch data to suggest flies for the current or forecast conditions

## Planned stack
Python, PostgreSQL or DuckDB, dbt, Dagster, Docker, GitHub Actions

## Status
In progress: setting up infrastructure.
