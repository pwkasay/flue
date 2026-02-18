"""FastAPI REST API for gridcarbon.

Endpoints:
  GET /                     → API info and status
  GET /now                  → Current carbon intensity + recommendation
  GET /forecast             → 24-hour forecast with cleanest/dirtiest windows
  GET /history              → Historical carbon intensity data
  GET /factors              → Emission factors used for calculations
  GET /health               → Health check
"""


import asyncio
from datetime import date, datetime
from typing import Any

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from ..models.fuel_mix import CarbonIntensity
from ..sources.nyiso import fetch_latest
from ..sources.weather import fetch_forecast as fetch_weather_forecast
from ..sources.emission_factors import all_factors_summary
from ..forecaster.heuristic import HeuristicForecaster
from ..storage.store import Store

app = FastAPI(
    title="gridcarbon",
    description="Real-time carbon intensity tracking and forecasting for the NYISO grid",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Shared state (initialized on startup) ──

_store: Store | None = None
_forecaster: HeuristicForecaster | None = None


def get_store() -> Store:
    global _store
    if _store is None:
        _store = Store()
    return _store


def get_forecaster() -> HeuristicForecaster:
    global _forecaster
    if _forecaster is None:
        _forecaster = HeuristicForecaster(get_store())
    return _forecaster


@app.on_event("startup")
async def startup() -> None:
    get_store()
    get_forecaster()


# ── Endpoints ──


@app.get("/")
async def root() -> dict[str, Any]:
    store = get_store()
    count = store.record_count()
    earliest, latest = store.date_range()
    return {
        "name": "gridcarbon",
        "version": "0.1.0",
        "region": "NYISO",
        "description": "Carbon intensity tracking and forecasting for the NYC electrical grid",
        "data": {
            "records": count,
            "earliest": earliest,
            "latest": latest,
        },
        "endpoints": ["/now", "/forecast", "/history", "/factors", "/health"],
    }


@app.get("/now")
async def current_intensity() -> dict[str, Any]:
    """Get the current grid carbon intensity with recommendation."""
    # Try live data first
    try:
        latest_mix = await fetch_latest()
        if latest_mix:
            ci = latest_mix.carbon_intensity
            # Also save it
            try:
                get_store().save_fuel_mix(latest_mix)
            except Exception:
                pass

            return {
                "timestamp": latest_mix.timestamp.isoformat(),
                "carbon_intensity": {
                    "grams_co2_per_kwh": round(ci.grams_co2_per_kwh, 1),
                    "kg_co2_per_mwh": round(ci.kg_co2_per_mwh, 1),
                    "category": ci.category,
                    "label": ci.category_label,
                },
                "recommendation": ci.recommendation,
                "generation": {
                    "total_mw": round(latest_mix.total_generation_mw, 1),
                    "clean_percentage": round(latest_mix.clean_percentage, 1),
                    "fuel_breakdown_mw": {
                        k: round(v, 1) for k, v in latest_mix.fuel_breakdown.items()
                    },
                    "fuel_percentages": latest_mix.fuel_percentages,
                },
                "source": "nyiso_realtime",
            }
    except Exception:
        pass

    # Fall back to stored data
    stored = get_store().get_latest_intensity()
    if stored:
        ci_val = stored["grams_co2_per_kwh"]
        ci = CarbonIntensity(grams_co2_per_kwh=ci_val)
        return {
            "timestamp": stored["timestamp"],
            "carbon_intensity": {
                "grams_co2_per_kwh": round(ci_val, 1),
                "category": ci.category,
                "label": ci.category_label,
            },
            "recommendation": ci.recommendation,
            "source": "stored",
        }

    raise HTTPException(
        status_code=503,
        detail="No carbon intensity data available. Run 'gridcarbon seed' first.",
    )


@app.get("/forecast")
async def forecast(
    hours: int = Query(default=24, ge=1, le=48),
    window_hours: int = Query(default=3, ge=1, le=12),
) -> dict[str, Any]:
    """Get carbon intensity forecast with cleanest/dirtiest windows."""
    forecaster = get_forecaster()

    # Get current intensity for persistence blend
    current_ci = None
    try:
        latest_mix = await fetch_latest()
        if latest_mix:
            current_ci = latest_mix.carbon_intensity
    except Exception:
        pass

    # Get weather forecast for corrections
    weather = None
    try:
        weather = await fetch_weather_forecast(days=2)
    except Exception:
        pass

    fc = forecaster.forecast(
        hours=hours,
        weather=weather,
        current_intensity=current_ci,
    )

    result = fc.to_dict()

    # Add the clean-window recommendation for the requested window size
    cleanest = fc.cleanest_window(window_hours)
    dirtiest = fc.dirtiest_window(window_hours)
    result[f"cleanest_{window_hours}h_window"] = cleanest.to_dict() if cleanest else None
    result[f"dirtiest_{window_hours}h_window"] = dirtiest.to_dict() if dirtiest else None

    return result


@app.get("/history")
async def history(
    hours: int = Query(default=24, ge=1, le=720),
) -> dict[str, Any]:
    """Get historical carbon intensity data."""
    records = get_store().get_carbon_intensity(hours=hours)
    return {
        "hours": hours,
        "count": len(records),
        "records": records,
    }


@app.get("/factors")
async def emission_factors() -> dict[str, Any]:
    """Get the emission factors used for carbon intensity calculations."""
    return {
        "methodology": "direct_combustion",
        "source": "EPA eGRID 2022 + EIA derived factors for NYISO",
        "factors": all_factors_summary(),
    }


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
