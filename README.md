# gridcarbon

Real-time carbon intensity tracking and forecasting for the NYC / NYISO electrical grid. Fetches fuel mix data from NYISO, calculates grid carbon intensity using EPA eGRID emission factors, stores history in PostgreSQL, and forecasts the cleanest times to consume electricity.

## Prerequisites

- **Python 3.14+**
- **PostgreSQL 17+** (or use Docker, which handles this automatically)
- **weir** — installed from GitHub (not yet on PyPI):
  ```bash
  pip install git+https://github.com/pwkasay/weir.git
  ```

## Installation

```bash
git clone <repo-url> && cd gridcarbon
pip install -e ".[dev]"
```

## Quick Start

The fastest way to get running is Docker:

```bash
docker compose up --build
# API on http://localhost:8000, dashboard on http://localhost:3000
```

Or install locally (requires PostgreSQL running):

```bash
pip install -e ".[dev]"
alembic upgrade head
gridcarbon seed --days 7
gridcarbon serve
```

## Docker

```bash
# Build and start — API on :8000, dashboard on :3000
docker compose up --build

# Stop
docker compose down

# Stop and delete all data
docker compose down -v
```

The first launch seeds 7 days of NYISO data automatically. Data persists in a Docker volume across restarts. Open http://localhost:3000 for the Canary dashboard (falls back to demo data while the API seeds).

## Exposing to the Internet (ngrok)

You can expose the full stack (API + dashboard) to the internet using [ngrok](https://ngrok.com/).

### Prerequisites

1. Install ngrok: `brew install ngrok` (or [download](https://ngrok.com/download))
2. Authenticate: `ngrok config add-authtoken <your-token>`

### Steps

**1. Create an ngrok config with both tunnels** (`~/.config/ngrok/tunnels.yml`):

```yaml
tunnels:
  api:
    addr: 8000
    proto: http
  dashboard:
    addr: 3000
    proto: http
```

**2. Start both tunnels:**

```bash
ngrok start --config ~/.config/ngrok/tunnels.yml --all
```

Note the two forwarding URLs ngrok assigns (e.g. `https://abc123.ngrok-free.app` for the API).

**3. Rebuild the dashboard with the ngrok API URL:**

```bash
# Stop the dashboard container, rebuild with the ngrok API base, restart
docker compose stop dashboard
docker compose build --build-arg VITE_API_BASE=https://<your-api-subdomain>.ngrok-free.app dashboard
docker compose up -d dashboard
```

The `VITE_API_BASE` build arg tells Vite to compile the dashboard with your ngrok API URL instead of `localhost:8000`. This is needed because Vite env vars are baked in at build time.

**4. Share the dashboard tunnel URL** — anyone with the link can view the Canary dashboard.

### ngrok browser warning bypass

ngrok's free tier shows an interstitial warning page on first visit. The codebase already handles this automatically:

- **API** — `NgrokBypassMiddleware` in `app.py` adds the `ngrok-skip-browser-warning` response header to all API responses
- **Dashboard** — `nginx.conf` adds the same header to all static file responses

No extra configuration needed.

### Cleanup

When done, stop ngrok (`Ctrl+C`) and optionally rebuild the dashboard without the build arg to restore localhost defaults:

```bash
docker compose stop dashboard
docker compose build dashboard
docker compose up -d dashboard
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `gridcarbon now` | Current carbon intensity, fuel mix breakdown, and recommendation |
| `gridcarbon forecast` | 24-hour forecast with cleanest/dirtiest windows |
| `gridcarbon seed --days N` | Backfill N days of historical data from NYISO |
| `gridcarbon ingest` | Continuous ingestion (polls NYISO every 5 minutes) |
| `gridcarbon serve` | Start FastAPI server on http://127.0.0.1:8000 |
| `gridcarbon status` | Database record count and date range |

All commands accept `--verbose / -v` for debug logging.

## API Endpoints

Start the server with `gridcarbon serve`, then:

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | API info and data coverage |
| GET | `/now` | Current carbon intensity + recommendation |
| GET | `/forecast?hours=24&window_hours=3` | Forecast with cleanest/dirtiest windows |
| GET | `/history?hours=24` | Historical carbon intensity records |
| GET | `/factors` | Emission factors used in calculations |
| GET | `/health` | Health check |
| GET | `/admin/status` | Ingestion status (connector health, record counts) |
| GET | `/admin/events?limit=50&event_type=...` | Recent ingestion events (failures, starts/stops) |

### Examples

```bash
# Current intensity
curl http://localhost:8000/now

# 12-hour forecast with 2-hour clean windows
curl "http://localhost:8000/forecast?hours=12&window_hours=2"

# Last 48 hours of history
curl "http://localhost:8000/history?hours=48"
```

## Running Tests

```bash
# Unit tests (domain models, storage, forecaster — no weir needed)
pytest tests/test_gridcarbon.py

# Pipeline integration tests (requires weir)
pytest tests/test_pipeline_integration.py

# All tests
pytest

# Lint and format
ruff check src/ tests/
ruff format --check src/ tests/
```

## Architecture

```
NYISO (CSV, 5-min intervals)
    │
    ▼
weir Pipeline
    │
    ├── validate (≥3 fuels, positive generation, no negatives)
    │
    └── persist (PostgreSQL, AsyncStore)
            │
            ▼
       PostgreSQL
       ┌────┼────────┐
       ▼    ▼        ▼
     CLI  API    Forecaster
      │    │
      │    └── /admin/status, /admin/events
      │
      └── Canary Dashboard (React, :3000)
```

**Data flow**: NYISO CSV → parse → FuelMix domain model (carbon intensity computed at init) → weir pipeline (validate → persist) → PostgreSQL → CLI / API / Forecaster / Dashboard.

## Project Structure

```
src/gridcarbon/
├── models/
│   ├── carbon_intensity.py   # CarbonIntensity unit class (gCO2/kWh canonical)
│   ├── fuel_mix.py           # FuelMix snapshot — one 5-min NYISO interval
│   ├── forecast.py           # Forecast result with hourly predictions
│   └── exceptions.py         # Exception hierarchy (Syntactic/Semantic/DataSource)
├── sources/
│   ├── nyiso.py              # NYISO CSV fetcher (sync + async)
│   ├── weather.py            # Open-Meteo weather data (for forecast corrections)
│   └── emission_factors.py   # Static EPA eGRID emission factor registry
├── pipeline/
│   └── ingest.py             # weir stages: validate, persist
├── forecaster/
│   └── heuristic.py          # Historical avg + temp/wind corrections
├── storage/
│   ├── store.py              # Sync PostgreSQL (psycopg3) — CLI/forecaster
│   └── async_store.py        # Async PostgreSQL (asyncpg) — pipeline/API
├── api/
│   └── app.py                # FastAPI REST + admin endpoints
└── cli/
    └── main.py               # Typer CLI with Rich output

dashboard/
├── src/
│   ├── App.jsx               # Main dashboard (live intensity, forecast, fuel mix)
│   ├── AdminPage.jsx         # Ingestion monitoring (connector status, failures)
│   ├── shared.jsx            # Shared components (Card, StatusBadge, LiveDot)
│   ├── config.js             # API_BASE via VITE_API_BASE env var
│   └── main.jsx              # React entry point
├── Dockerfile                # Multi-stage: Node build → nginx serve
└── package.json

alembic/
├── env.py                    # Reads DATABASE_URL, rewrites for psycopg driver
└── versions/
    └── 001_initial_schema.py # fuel_mix, carbon_intensity, weather, ingestion_events

Dockerfile                    # Python 3.14-slim backend image
docker-compose.yml            # postgres + gridcarbon + ingest + dashboard
entrypoint.sh                 # Migrations → seed → serve
```

## Carbon Intensity Calculation

Average carbon intensity from NYISO fuel mix:

```
CI (gCO2/kWh) = Sum(Generation_i * EmissionFactor_i) / Sum(Generation_i)
```

Emission factors (direct combustion, EPA eGRID / EIA derived):

| NYISO Fuel Category | Factor (gCO2/kWh) | Notes |
|---------------------|-------------------|-------|
| Natural Gas | 450 | Weighted NY fleet avg (CCGT + peakers) |
| Dual Fuel | 480 | Mostly gas w/ oil backup, NYC plants |
| Nuclear | 0 | Zero direct emissions |
| Hydro | 0 | Zero direct emissions |
| Wind | 0 | Zero direct emissions |
| Other Renewables | 0 | Biomass treated as carbon-neutral |
| Other Fossil Fuels | 840 | Oil + residual coal |

## Heuristic Forecasting

No ML. Uses historical average CI for each (month, day_of_week, hour) combination, then applies:

1. **Temperature correction** — heating/cooling demand drives gas dispatch
2. **Persistence blend** — short-horizon forecasts weight current actual CI heavily
3. **Wind adjustment** — higher wind speed displaces gas generation

Falls back to a hardcoded `TYPICAL_HOURLY_PROFILE` when historical data is insufficient.

## Configuration

```bash
cp .env.example .env
```

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://gridcarbon:gridcarbon@localhost:5432/gridcarbon` | PostgreSQL connection string |
| `VITE_API_BASE` | `http://localhost:8000` | Dashboard API base URL |

## Not Yet Implemented

- **EIA API** — hourly generation by fuel type (alternative/supplement to NYISO)
- **WattTime** — marginal emissions validation
- **ElectricityMaps** — cross-validation data source
- **ML forecaster** — gradient-boosted or neural network model

## License

MIT
