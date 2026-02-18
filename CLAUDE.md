# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**gridcarbon** — Real-time carbon intensity tracking and forecasting for the NYC/NYISO electrical grid. Fetches fuel mix data from NYISO, calculates grid carbon intensity using EPA eGRID emission factors, stores historical data in SQLite, and forecasts cleanest electricity consumption windows using a heuristic model. The ingestion pipeline is built on **asyncpipe**, demonstrating the two projects composing together.

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

# CLI usage (after install)
gridcarbon now              # Current carbon intensity
gridcarbon forecast         # 24-hour forecast
gridcarbon seed --days 30   # Seed historical data
gridcarbon serve            # Start FastAPI server on :8000
gridcarbon ingest           # Continuous ingestion (polls every 5min)
gridcarbon status           # Database stats
```

## Architecture

The data flow is linear: **NYISO CSV → parse → FuelMix domain model (CI computed at init) → asyncpipe pipeline (validate → persist) → SQLite → CLI/API/Forecaster**.

### Key Design Patterns

- **Cloverly unit-class pattern**: Domain models (`CarbonIntensity`, `FuelMix`) store a single canonical internal unit and expose named properties for conversions. `CarbonIntensity` stores `grams_co2_per_kwh` and exposes `kg_co2_per_mwh`, `lbs_co2_per_mwh`, etc. These classes support arithmetic operators (`__add__`, `__truediv__`, `__lt__`) for composability.
- **Eager computation**: `FuelMix.__post_init__` computes carbon intensity immediately at construction — no lazy evaluation.
- **Exception hierarchy** (`models/exceptions.py`): `SyntacticException` (malformed input → HTTP 400) vs `SemanticException` (valid format, invalid meaning → HTTP 422), plus `DataSourceError` subtypes per external service. Pipeline adds `ValidationError` (extends `GridCarbonException`) for data quality failures.
- **Sync + async interfaces**: NYISO source provides both `fetch_fuel_mix_sync` (for CLI/seeding) and `fetch_fuel_mix_async` (for pipeline). CLI commands wrap async calls in `asyncio.run()`.

### asyncpipe Integration (pipeline/ingest.py)

The pipeline module is where asyncpipe and gridcarbon compose. Key patterns:

- **Stages are `@stage`-decorated async functions**: `validate` checks data quality (positive generation, ≥3 fuel categories, no negatives). Stages are independently testable — just `await validate(mix)` in tests.
- **Stage factory for runtime state**: `make_persist_stage(store)` returns a `@stage`-decorated function that closes over a `Store` instance. This is needed because `@stage` freezes the function at decoration time, but the Store path comes from CLI args/env vars at runtime.
- **Stage configuration**: `concurrency=1` on persist (SQLite single-writer). `retries=2, retry_base_delay=0.1` on persist for transient SQLite lock errors.
- **Pipeline builder pattern**: `Pipeline("name").source(async_gen).then(validate).then(persist).on_error(ValidationError).build().run()` — returns `PipelineResult` with per-stage metrics (items in/out/errored, latency percentiles, throughput).
- **Error routing**: `ValidationError` and `StoreError` go to dead letter collector. `NYISOFetchError` is caught at the source level (skips bad days, doesn't stop pipeline).
- **Two pipeline configs**: `build_seed_pipeline` (batch historical backfill, `channel_capacity=128`) and `build_continuous_pipeline` (infinite polling, `channel_capacity=16`).

### Module Responsibilities

- `sources/` — External data adapters (NYISO, Open-Meteo weather). NYISO is the primary source; weather is optional. EIA is not yet implemented (`EIAFetchError` exists in exceptions.py as a placeholder). `emission_factors.py` is the static factor registry — edit factors there when better data is available.
- `models/` — Immutable domain models. `FuelMix` holds a snapshot (one 5-min NYISO interval). `CarbonIntensity` is the core unit class. `Forecast` contains hourly predictions with sliding-window cleanest/dirtiest analysis.
- `storage/store.py` — Raw sqlite3, no ORM. Uses WAL mode. Two query patterns: time-series retrieval and hourly-average lookups (for the forecaster baseline).
- `forecaster/heuristic.py` — No ML. Uses historical average CI by (month, day_of_week, hour) + temperature/wind corrections + persistence blending for short horizons. Falls back to hardcoded `TYPICAL_HOURLY_PROFILE` when historical data is insufficient.
- `pipeline/ingest.py` — asyncpipe-based ingestion. Sources are async generators (`nyiso_date_source`, `continuous_source`). Stages are `validate` and `persist`. Runner functions (`run_seed`, `run_continuous`) return `PipelineResult`.
- `api/app.py` — FastAPI with lazy-initialized global `Store` and `HeuristicForecaster`. CORS enabled. Key endpoints: `/now`, `/forecast`, `/history`, `/factors`.
- `cli/main.py` — Typer CLI with Rich output. Entry point: `gridcarbon.cli.main:app`. The `seed` and `ingest` commands display `PipelineResult` stage metrics.

## Configuration

Environment variables (all optional — NYISO direct access works without any keys):
- `EIA_API_KEY` — EIA API access
- `WATTTIME_USERNAME` / `WATTTIME_PASSWORD` — WattTime validation
- `GRIDCARBON_DB_PATH` — SQLite path (default: `~/.gridcarbon/gridcarbon.db`)

## Testing

Two test files:
- `tests/test_gridcarbon.py` — Unit tests for domain models, emission factors, NYISO CSV parsing, storage, forecaster.
- `tests/test_pipeline_integration.py` — asyncpipe integration tests proving the two projects compose correctly. Tests pipeline flow, dead letter routing, stage metrics, and topology inspection.

pytest-asyncio is configured with `asyncio_mode = "auto"`. Tests use temp SQLite databases via `tempfile.mktemp()`. Test timeout is 30s. The `respx` library is available for mocking httpx requests. Pipeline stages can be tested standalone (just `await validate(mix)`) or as full pipelines.

## Tech Stack

Python ≥3.14, Hatchling build system. Key deps: asyncpipe (pipeline framework), httpx (HTTP), pydantic (FastAPI dependency), FastAPI/uvicorn, Typer/Rich (CLI). Ruff for linting (line-length 100, target py314). `from __future__ import annotations` is not used — Python 3.14 has PEP 649 (deferred annotation evaluation) built in.
