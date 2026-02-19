# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**gridcarbon** — Real-time carbon intensity tracking and forecasting for the NYC/NYISO electrical grid. Fetches fuel mix data from NYISO, calculates grid carbon intensity using EPA eGRID emission factors, stores historical data in PostgreSQL, and forecasts cleanest electricity consumption windows using a heuristic model. The Canary dashboard provides live visualization and admin monitoring. The ingestion pipeline is built on **weir**, demonstrating the two projects composing together.

## Commands

```bash
# Install (editable, with dev deps)
pip install -e ".[dev]"

# Run all tests
pytest

# Run a single test file or class or method
pytest tests/test_gridcarbon.py
pytest tests/test_pipeline_integration.py::TestSeedPipeline
pytest tests/test_gridcarbon.py::TestFuelMix::test_carbon_intensity_calculated_at_init

# Lint and format
ruff check src/ tests/
ruff format src/ tests/

# Type check
mypy src/

# Database migrations
alembic upgrade head

# Docker (simplest way to run everything)
docker compose up --build          # Build and start all services
docker compose down                # Stop services
docker compose down -v             # Stop and delete all data
docker compose logs -f gridcarbon  # Follow API logs

# CLI usage (after install, requires Postgres running)
gridcarbon now              # Current carbon intensity
gridcarbon forecast         # 24-hour forecast
gridcarbon seed --days 30   # Seed historical data
gridcarbon serve            # Start FastAPI server on :8000
gridcarbon ingest           # Continuous ingestion (polls every 5min)
gridcarbon status           # Database stats
```

## Architecture

The data flow is linear: **NYISO CSV → parse → FuelMix domain model (CI computed at init) → weir pipeline (validate → persist) → PostgreSQL → CLI/API/Forecaster/Dashboard**.

### Key Design Patterns

- **Cloverly unit-class pattern**: Domain models (`CarbonIntensity`, `FuelMix`) store a single canonical internal unit and expose named properties for conversions. `CarbonIntensity` stores `grams_co2_per_kwh` and exposes `kg_co2_per_mwh`, `lbs_co2_per_mwh`, etc. These classes support arithmetic operators (`__add__`, `__truediv__`, `__lt__`) for composability.
- **Eager computation**: `FuelMix.__post_init__` computes carbon intensity immediately at construction — no lazy evaluation.
- **Exception hierarchy** (`models/exceptions.py`): `SyntacticException` (malformed input → HTTP 400) vs `SemanticException` (valid format, invalid meaning → HTTP 422), plus `DataSourceError` subtypes per external service. Pipeline adds `ValidationError` (extends `GridCarbonException`) for data quality failures.
- **Sync + async interfaces**: NYISO source provides both `fetch_fuel_mix_sync` (for CLI/seeding) and `fetch_fuel_mix_async` (for pipeline). Storage splits into `Store` (psycopg3, sync — CLI/forecaster) and `AsyncStore` (asyncpg — pipeline/API). CLI commands wrap async calls in `asyncio.run()`.

### Storage

Dual sync/async pattern — `Store` (psycopg3, sync) for CLI/forecaster and `AsyncStore` (asyncpg, connection pool) for pipeline/API. Both provide identical methods (`save_fuel_mix`, `get_carbon_intensity`, `log_event`, etc.) but differ in driver conventions: psycopg3 uses `%s` parameter placeholders, asyncpg uses `$1`. Both auto-deserialize JSONB columns. `AsyncStore` is created via `await AsyncStore.create(dsn)` factory (pool `min_size=2, max_size=10`).

### Admin / Ingestion Events

The `ingestion_events` table provides an audit trail. `log_event(event_type, stage_name, message, details)` writes entries from both stores. Event types: `validation_failure`, `persist_failure`, `pipeline_start`, `pipeline_stop`. Admin API endpoints (`/admin/status`, `/admin/events`) expose this data for the dashboard.

### Dashboard (Canary)

React/Vite/Tailwind SPA served via nginx on `:3000`. Uses recharts for visualization (AreaChart for forecast, PieChart for fuel mix). Routes: `/` (live dashboard) and `/admin` (ingestion monitoring).

- `config.js` — `API_BASE` sourced from `VITE_API_BASE` env var (defaults to `http://localhost:8000`)
- `shared.jsx` — shared components (`Card`, `StatusBadge`, `LiveDot`, `Skeleton`), color systems (`INTENSITY_COLORS`, `FUEL_COLORS`), utilities (`fetchJSON`, `timeAgo`)
- `App.jsx` — main dashboard with 5-minute auto-refresh, mock data fallback when API is unavailable
- `AdminPage.jsx` — connector status cards, data freshness metrics, failures table (auto-refreshes every 30s)

### Docker

Multi-service architecture in `docker-compose.yml`:

- **postgres** — PostgreSQL 17 Alpine with healthcheck (`pg_isready`), data persisted in `pgdata` volume
- **gridcarbon** — API server. `entrypoint.sh` runs `alembic upgrade head`, seeds 7 days on first run, then starts `gridcarbon serve --host 0.0.0.0`
- **ingest** — continuous pipeline (`gridcarbon ingest --interval 300`), depends on healthy postgres + started gridcarbon
- **dashboard** — multi-stage Node build → nginx serving static SPA

### Alembic

Migrations use raw SQL (no ORM models). `alembic/env.py` reads `DATABASE_URL` from the environment and rewrites `postgresql://` → `postgresql+psycopg://` for SQLAlchemy driver selection. Schema: `fuel_mix`, `carbon_intensity`, `weather`, `ingestion_events` tables defined in `001_initial_schema.py`.

### weir Integration (pipeline/ingest.py)

The pipeline module is where weir and gridcarbon compose. Key patterns:

- **Stages are `@stage`-decorated async functions**: `validate` checks data quality (positive generation, ≥3 fuel categories, no negatives). Stages are independently testable — just `await validate(mix)` in tests.
- **Stage factory for runtime state**: `make_persist_stage(async_store)` returns a `@stage`-decorated function that closes over an `AsyncStore` instance. This is needed because `@stage` freezes the function at decoration time, but the Store DSN comes from CLI args/env vars at runtime.
- **Stage configuration**: `concurrency=1` on persist. `retries=2, retry_base_delay=0.1` on persist for transient Postgres errors.
- **Event logging handler**: `make_event_logging_handler(async_store)` creates an error handler that logs failures to `ingestion_events` for admin visibility.
- **Pipeline builder pattern**: `Pipeline("name").source(async_gen).then(validate).then(persist).on_error(ValidationError).build().run()` — returns `PipelineResult` with per-stage metrics (items in/out/errored, latency percentiles, throughput).
- **Error routing**: `ValidationError` and `StoreError` go to dead letter collector. `NYISOFetchError` is caught at the source level (skips bad days, doesn't stop pipeline).
- **Two pipeline configs**: `build_seed_pipeline` (batch historical backfill, `channel_capacity=128`) and `build_continuous_pipeline` (infinite polling, `channel_capacity=16`).

### Module Responsibilities

- `sources/` — External data adapters (NYISO, Open-Meteo weather). NYISO is the primary source; weather is optional. EIA is not yet implemented (`EIAFetchError` exists in exceptions.py as a placeholder). `emission_factors.py` is the static factor registry — edit factors there when better data is available.
- `models/` — Immutable domain models. `FuelMix` holds a snapshot (one 5-min NYISO interval). `CarbonIntensity` is the core unit class. `Forecast` contains hourly predictions with sliding-window cleanest/dirtiest analysis.
- `storage/store.py` — Sync PostgreSQL (psycopg3), no ORM. Two query patterns: time-series retrieval and hourly-average lookups (for the forecaster baseline). New: `log_event`, `get_recent_events`, `get_ingestion_status` for admin.
- `storage/async_store.py` — Async PostgreSQL (asyncpg) with connection pool. Same API as sync Store. Used by pipeline and API.
- `forecaster/heuristic.py` — No ML. Uses historical average CI by (month, day_of_week, hour) + temperature/wind corrections + persistence blending for short horizons. Falls back to hardcoded `TYPICAL_HOURLY_PROFILE` when historical data is insufficient.
- `pipeline/ingest.py` — weir-based ingestion. Sources are async generators (`nyiso_date_source`, `continuous_source`). Stages are `validate` and `persist`. Runner functions (`run_seed`, `run_continuous`) return `PipelineResult`.
- `api/app.py` — FastAPI with lifespan-managed `AsyncStore` pool and sync `Store` for forecaster. CORS enabled. Key endpoints: `/now`, `/forecast`, `/history`, `/factors`, `/admin/status`, `/admin/events`.
- `cli/main.py` — Typer CLI with Rich output. Entry point: `gridcarbon.cli.main:app`. The `seed` and `ingest` commands display `PipelineResult` stage metrics.

## Configuration

Environment variables (all optional — NYISO direct access works without any keys):
- `DATABASE_URL` — PostgreSQL connection string (default: `postgresql://gridcarbon:gridcarbon@localhost:5432/gridcarbon`)
- `TEST_DATABASE_URL` — Postgres DSN for tests (defaults to `gridcarbon_test` database)
- `VITE_API_BASE` — Dashboard API base URL (defaults to `http://localhost:8000`)
- `EIA_API_KEY` — EIA API access
- `WATTTIME_USERNAME` / `WATTTIME_PASSWORD` — WattTime validation

## Testing

Three test files:
- `tests/conftest.py` — Shared Postgres fixtures (sync_store, async_store) with table truncation for isolation. Skips gracefully if Postgres unavailable.
- `tests/test_gridcarbon.py` — Unit tests for domain models, emission factors, NYISO CSV parsing, storage, forecaster.
- `tests/test_pipeline_integration.py` — weir integration tests proving the two projects compose correctly. Tests pipeline flow, dead letter routing, stage metrics, event logging, and topology inspection.

pytest-asyncio is configured with `asyncio_mode = "auto"`. Tests use a Postgres test database (`TEST_DATABASE_URL` env var, defaults to `gridcarbon_test`). Test timeout is 30s. The `respx` library is available for mocking httpx requests. Pipeline stages can be tested standalone (just `await validate(mix)`) or as full pipelines.

## Tech Stack

Python ≥3.14, Hatchling build system. Key deps: weir (pipeline framework), httpx (HTTP), pydantic (FastAPI dependency), FastAPI/uvicorn, Typer/Rich (CLI), psycopg3 (sync Postgres), asyncpg (async Postgres), Alembic (migrations). Dashboard: React 18, Vite, Tailwind CSS, recharts, react-router-dom. Ruff for linting (line-length 100, target py314). `from __future__ import annotations` is not used — Python 3.14 has PEP 649 (deferred annotation evaluation) built in.
