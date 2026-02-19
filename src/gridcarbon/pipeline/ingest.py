"""Ingestion pipeline built on weir (Sluice).

This is where the two portfolio projects meet. The pipeline uses weir's
stage decorator, error routing, backpressure, and metrics collection to
ingest NYISO fuel mix data and Open-Meteo weather data.

Four pipeline configurations:
1. NYISO seed pipeline — batch historical fuel mix import
2. NYISO continuous pipeline — poll every 5 minutes for latest data
3. Weather seed pipeline — batch historical weather import
4. Weather continuous pipeline — poll hourly for forecast data

All use the same pattern: async generator source → validate → persist,
wired through weir's Pipeline builder. New weir v0.4.0 features used:
- batch_stage for weather persist (hourly data arrives in bursts)
- Hook protocol for lifecycle logging on continuous pipelines
- on_metrics streaming for admin dashboard observability

Architecture:
    NYISO source (async gen, yields FuelMix)
      → validate  (check data quality)
      → persist   (Postgres write via AsyncStore)

    Weather source (async gen, yields WeatherSnapshot)
      → validate_weather  (plausibility checks)
      → weather_persist   (batch write via AsyncStore)

Error strategy:
    - ValidationError → event logged + dead lettered
    - NYISOFetchError → logged in source, skipped (source-level resilience)
    - WeatherFetchError → logged in source, skipped
    - StoreError → retried once, then event logged + dead-lettered
"""

import asyncio
import logging
from datetime import date, timedelta
from typing import Any, AsyncIterator

import httpx

from weir import FailedItem, Pipeline, PipelineResult, StageMetricsSnapshot, batch_stage, stage

from ..models.fuel_mix import FuelMix
from ..models.exceptions import (
    GridCarbonException,
    NYISOFetchError,
    StoreError,
    WeatherFetchError,
)
from ..sources.nyiso import fetch_fuel_mix_async, fetch_latest
from ..sources.weather import WeatherSnapshot, fetch_forecast, fetch_historical
from ..storage.async_store import AsyncStore

logger = logging.getLogger("gridcarbon.pipeline")


# ─── Exceptions (pipeline-specific) ───


class ValidationError(GridCarbonException):
    """A FuelMix or WeatherSnapshot record failed validation."""

    pass


# ─── Error Handlers ───


def make_event_logging_handler(async_store: AsyncStore):
    """Create an error handler that logs failures to the ingestion_events table.

    Returns an async callable matching weir's ErrorHandler signature:
    async def(failed: FailedItem) -> None
    """

    async def handler(failed: FailedItem) -> None:
        event_type = f"{failed.stage_name}_failure"
        await async_store.log_event(
            event_type=event_type,
            stage_name=failed.stage_name,
            message=str(failed.error),
            details={"error": str(failed.error), "attempts": failed.attempts},
        )

    return handler


# ─── Lifecycle Hook (weir v0.4.0) ───


class LoggingHook:
    """Lifecycle hook that logs pipeline events to ingestion_events.

    Implements weir's Hook protocol — on_start, on_error, on_complete.
    Wired into continuous pipelines for admin visibility.
    """

    def __init__(self, async_store: AsyncStore) -> None:
        self._store = async_store

    async def on_start(self, stage_name: str) -> None:
        await self._store.log_event(
            event_type="stage_start",
            stage_name=stage_name,
            message=f"Stage '{stage_name}' started",
        )

    async def on_error(self, stage_name: str, item: Any, error: Exception) -> None:
        await self._store.log_event(
            event_type="stage_error",
            stage_name=stage_name,
            message=str(error),
            details={"error_type": type(error).__name__, "error": str(error)},
        )

    async def on_complete(self, stage_name: str) -> None:
        await self._store.log_event(
            event_type="stage_complete",
            stage_name=stage_name,
            message=f"Stage '{stage_name}' completed",
        )


# ─── Metrics Callback (weir v0.4.0) ───


def make_metrics_callback(async_store: AsyncStore, pipeline_name: str):
    """Create an on_metrics callback that persists stage snapshots.

    Returns an async callable for weir's .on_metrics() — receives
    a list of StageMetricsSnapshot dicts each interval.
    """

    async def callback(snapshots: list[StageMetricsSnapshot]) -> None:
        await async_store.save_pipeline_metrics(pipeline_name, snapshots)

    return callback


# ─── NYISO Sources (async generators) ───


async def nyiso_date_source(
    start: date,
    end: date,
    rate_limit_delay: float = 0.5,
    progress_callback: Any | None = None,
) -> AsyncIterator[FuelMix]:
    """Async generator that yields individual FuelMix objects for a date range.

    This is the weir source for the seed pipeline. It handles:
    - Day-by-day fetching from NYISO (predictable URL per date)
    - Rate limiting (polite delay between requests)
    - Per-day error resilience (one bad day doesn't stop the pipeline)
    - Progress reporting via callback

    Yields individual FuelMix objects (not lists), so the pipeline
    processes them one at a time with proper backpressure.
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        current = start
        days_fetched = 0
        while current <= end:
            try:
                mixes = await fetch_fuel_mix_async(current, client=client)
                days_fetched += 1

                if progress_callback:
                    progress_callback(current, len(mixes))

                for mix in mixes:
                    yield mix

                logger.debug("Source yielded %d records for %s", len(mixes), current.isoformat())

            except NYISOFetchError as e:
                logger.warning("Source skipping %s: %s", current.isoformat(), e)

            # Rate limit — be polite to NYISO
            await asyncio.sleep(rate_limit_delay)
            current += timedelta(days=1)

        logger.info("Source exhausted: %d days fetched", days_fetched)


async def continuous_source(
    async_store: AsyncStore,
    poll_interval: float = 300.0,
) -> AsyncIterator[FuelMix]:
    """Infinite async generator that polls NYISO for the latest fuel mix.

    This is the weir source for continuous ingestion.
    Yields one FuelMix every poll_interval seconds.
    Runs until the pipeline is shut down (Ctrl+C triggers weir's
    graceful shutdown via signal handlers).
    """
    logger.info("Continuous source starting (poll every %.0fs)", poll_interval)
    while True:
        try:
            latest = await fetch_latest()
            if latest:
                yield latest
                logger.debug(
                    "Polled: %.0f gCO₂/kWh at %s",
                    latest.carbon_intensity.grams_co2_per_kwh,
                    latest.timestamp.strftime("%H:%M"),
                )
            else:
                logger.warning("Poll returned no data")
        except Exception as e:
            logger.error("Poll error: %s", e)

        await asyncio.sleep(poll_interval)


# ─── Weather Sources (async generators) ───


async def weather_historical_source(
    start: date,
    end: date,
    rate_limit_delay: float = 1.0,
) -> AsyncIterator[WeatherSnapshot]:
    """Async generator yielding WeatherSnapshot objects for a date range.

    Fetches one day at a time from Open-Meteo's archive API with rate
    limiting. Matches the nyiso_date_source pattern.
    """
    current = start
    days_fetched = 0
    while current <= end:
        try:
            snapshots = await fetch_historical(current, current)
            days_fetched += 1
            for snapshot in snapshots:
                yield snapshot
            logger.debug(
                "Weather source yielded %d records for %s", len(snapshots), current.isoformat()
            )
        except WeatherFetchError as e:
            logger.warning("Weather source skipping %s: %s", current.isoformat(), e)

        await asyncio.sleep(rate_limit_delay)
        current += timedelta(days=1)

    logger.info("Weather source exhausted: %d days fetched", days_fetched)


async def weather_continuous_source(
    poll_interval: float = 3600.0,
) -> AsyncIterator[WeatherSnapshot]:
    """Infinite async generator polling Open-Meteo forecast hourly.

    Fetches the next day's forecast and yields individual WeatherSnapshot
    objects. Runs until the pipeline is shut down.
    """
    logger.info("Weather continuous source starting (poll every %.0fs)", poll_interval)
    while True:
        try:
            snapshots = await fetch_forecast(days=1)
            for snapshot in snapshots:
                yield snapshot
            logger.debug("Weather poll yielded %d snapshots", len(snapshots))
        except WeatherFetchError as e:
            logger.error("Weather poll error: %s", e)
        except Exception as e:
            logger.error("Weather poll unexpected error: %s", e)

        await asyncio.sleep(poll_interval)


# ─── NYISO Pipeline Stages ───
#
# Each stage is a pure async function decorated with @stage.
# They can be tested independently — just call them directly.
# The Pipeline wires them together with channels, error routing,
# and metrics collection.


@stage(concurrency=1)
async def validate(mix: FuelMix) -> FuelMix:
    """Validate a FuelMix record.

    Checks:
    - Total generation is positive (zero = bad data or outage)
    - At least 3 fuel categories present (NYISO reports 7 normally)
    - No individual fuel has negative generation

    Invalid records are raised as ValidationError and routed to the
    dead letter collector by the pipeline's error router.
    """
    if mix.total_generation_mw <= 0:
        raise ValidationError(
            f"Zero/negative total generation ({mix.total_generation_mw} MW) "
            f"at {mix.timestamp.isoformat()}"
        )

    if len(mix.fuels) < 3:
        raise ValidationError(
            f"Only {len(mix.fuels)} fuel categories at {mix.timestamp.isoformat()} (expected ≥3)"
        )

    for fuel in mix.fuels:
        if fuel.generation_mw < 0:
            raise ValidationError(
                f"Negative generation ({fuel.generation_mw} MW) for "
                f"{fuel.fuel.value} at {mix.timestamp.isoformat()}"
            )

    return mix


def make_persist_stage(async_store: AsyncStore):
    """Factory for the persist stage — closes over the AsyncStore instance.

    Why a factory? The @stage decorator freezes the function at decoration
    time, but the AsyncStore is created at runtime (its DSN might come from
    CLI args or env vars). So we create the decorated stage dynamically.
    """

    @stage(concurrency=1, retries=2, retry_base_delay=0.1)
    async def persist(mix: FuelMix) -> FuelMix:
        """Persist a validated FuelMix to the Postgres store.

        concurrency=1: Single-writer to avoid contention.

        retries=2: Transient Postgres errors are retried once with a short
        delay. Persistent failures go to dead letters.
        """
        try:
            await async_store.save_fuel_mix(mix)
        except StoreError:
            raise  # Let weir's retry machinery handle it
        except Exception as e:
            raise StoreError(f"Unexpected persist error: {e}") from e
        return mix

    return persist


# ─── Weather Pipeline Stages ───


@stage(concurrency=1)
async def validate_weather(snapshot: WeatherSnapshot) -> WeatherSnapshot:
    """Validate a WeatherSnapshot for plausible values.

    Checks:
    - Temperature in range -40°F to 130°F (physical plausibility)
    - Wind speed >= 0
    - Cloud cover in 0–100%
    """
    if not -40 <= snapshot.temperature_f <= 130:
        raise ValidationError(
            f"Implausible temperature {snapshot.temperature_f}°F "
            f"at {snapshot.timestamp.isoformat()}"
        )

    if snapshot.wind_speed_80m_mph < 0:
        raise ValidationError(
            f"Negative wind speed {snapshot.wind_speed_80m_mph} mph "
            f"at {snapshot.timestamp.isoformat()}"
        )

    if not 0 <= snapshot.cloud_cover_pct <= 100:
        raise ValidationError(
            f"Cloud cover {snapshot.cloud_cover_pct}% out of range "
            f"at {snapshot.timestamp.isoformat()}"
        )

    return snapshot


def make_weather_persist_stage(async_store: AsyncStore):
    """Factory for the weather persist stage using weir's batch_stage.

    Uses batch_stage because weather data arrives in hourly bursts (24
    snapshots per forecast fetch). Batching reduces Postgres round-trips.
    """

    @batch_stage(batch_size=24, flush_timeout=5.0, concurrency=1, retries=2, retry_base_delay=0.1)
    async def weather_persist(snapshots: list[WeatherSnapshot]) -> list[WeatherSnapshot]:
        """Batch-persist validated WeatherSnapshots to the Postgres store."""
        for snapshot in snapshots:
            try:
                await async_store.save_weather(
                    timestamp=snapshot.timestamp,
                    temp_f=snapshot.temperature_f,
                    wind_mph=snapshot.wind_speed_80m_mph,
                    cloud_pct=snapshot.cloud_cover_pct,
                )
            except StoreError:
                raise
            except Exception as e:
                raise StoreError(f"Unexpected weather persist error: {e}") from e
        return snapshots

    return weather_persist


# ─── NYISO Pipeline Builders ───


def build_seed_pipeline(
    async_store: AsyncStore,
    start: date,
    end: date,
    channel_capacity: int = 128,
    progress_callback: Any | None = None,
) -> Pipeline:
    """Build the historical NYISO seed pipeline.

    Returns a built (but not yet running) Pipeline.

    Architecture:
        nyiso_date_source → validate → persist
        ValidationError ──→ event log + dead letters
        StoreError ────────→ event log + dead letters (after 1 retry)
    """
    handler = make_event_logging_handler(async_store)
    return (
        Pipeline(
            "gridcarbon-seed",
            channel_capacity=channel_capacity,
            drain_timeout=60.0,
            log_level=logging.INFO,
        )
        .source(nyiso_date_source(start, end, progress_callback=progress_callback))
        .then(validate)
        .then(make_persist_stage(async_store))
        .on_error(ValidationError, handler)
        .on_error(StoreError, handler)
        .on_metrics(make_metrics_callback(async_store, "gridcarbon-seed"), interval=10.0)
        .build()
    )


def build_continuous_pipeline(
    async_store: AsyncStore,
    poll_interval: float = 300.0,
    channel_capacity: int = 16,
) -> Pipeline:
    """Build the continuous NYISO ingestion pipeline.

    Runs until Ctrl+C. weir installs signal handlers for graceful
    shutdown: the source stops yielding, in-flight items drain through
    the stages, and the pipeline returns a result summary.

    Architecture:
        continuous_source → validate → persist
    """
    handler = make_event_logging_handler(async_store)
    return (
        Pipeline(
            "gridcarbon-ingest",
            channel_capacity=channel_capacity,
            drain_timeout=15.0,
            log_level=logging.INFO,
        )
        .source(continuous_source(async_store, poll_interval=poll_interval))
        .then(validate)
        .then(make_persist_stage(async_store))
        .on_error(ValidationError, handler)
        .on_error(StoreError, handler)
        .on_metrics(make_metrics_callback(async_store, "gridcarbon-ingest"), interval=10.0)
        .with_hook(LoggingHook(async_store))
        .build()
    )


# ─── Weather Pipeline Builders ───


def build_weather_seed_pipeline(
    async_store: AsyncStore,
    start: date,
    end: date,
    channel_capacity: int = 128,
) -> Pipeline:
    """Build the historical weather seed pipeline.

    Architecture:
        weather_historical_source → validate_weather → weather_persist (batch)
    """
    handler = make_event_logging_handler(async_store)
    return (
        Pipeline(
            "gridcarbon-weather-seed",
            channel_capacity=channel_capacity,
            drain_timeout=60.0,
            log_level=logging.INFO,
        )
        .source(weather_historical_source(start, end))
        .then(validate_weather)
        .then(make_weather_persist_stage(async_store))
        .on_error(ValidationError, handler)
        .on_error(StoreError, handler)
        .on_metrics(make_metrics_callback(async_store, "gridcarbon-weather-seed"), interval=10.0)
        .build()
    )


def build_weather_continuous_pipeline(
    async_store: AsyncStore,
    poll_interval: float = 3600.0,
    channel_capacity: int = 16,
) -> Pipeline:
    """Build the continuous weather ingestion pipeline.

    Polls Open-Meteo hourly for forecast data.

    Architecture:
        weather_continuous_source → validate_weather → weather_persist (batch)
    """
    handler = make_event_logging_handler(async_store)
    return (
        Pipeline(
            "gridcarbon-weather",
            channel_capacity=channel_capacity,
            drain_timeout=15.0,
            log_level=logging.INFO,
        )
        .source(weather_continuous_source(poll_interval=poll_interval))
        .then(validate_weather)
        .then(make_weather_persist_stage(async_store))
        .on_error(ValidationError, handler)
        .on_error(StoreError, handler)
        .on_metrics(make_metrics_callback(async_store, "gridcarbon-weather"), interval=10.0)
        .with_hook(LoggingHook(async_store))
        .build()
    )


# ─── Runner Functions (called by CLI) ───


async def run_seed(
    async_store: AsyncStore,
    start: date,
    end: date,
    progress_callback: Any | None = None,
    include_weather: bool = True,
) -> tuple[PipelineResult, PipelineResult | None]:
    """Seed historical data using weir pipelines.

    Returns (nyiso_result, weather_result). weather_result is None if
    include_weather is False.
    """
    nyiso_pipeline = build_seed_pipeline(
        async_store, start, end, progress_callback=progress_callback
    )
    logger.info("NYISO seed pipeline topology:\n%s", nyiso_pipeline.topology)

    if include_weather:
        weather_pipeline = build_weather_seed_pipeline(async_store, start, end)
        logger.info("Weather seed pipeline topology:\n%s", weather_pipeline.topology)

        nyiso_result, weather_result = await asyncio.gather(
            nyiso_pipeline.run(),
            weather_pipeline.run(),
        )
        logger.info("NYISO seed complete:\n%s", nyiso_result.summary())
        logger.info("Weather seed complete:\n%s", weather_result.summary())
        return nyiso_result, weather_result
    else:
        result = await nyiso_pipeline.run()
        logger.info("NYISO seed complete:\n%s", result.summary())
        return result, None


async def run_continuous(
    async_store: AsyncStore,
    poll_interval_seconds: int = 300,
    weather_poll_interval_seconds: int = 3600,
    **kwargs: Any,
) -> tuple[PipelineResult, PipelineResult]:
    """Run continuous ingestion using weir pipelines.

    Runs NYISO + weather pipelines concurrently until interrupted (Ctrl+C).
    Returns (nyiso_result, weather_result) on shutdown.
    """
    await async_store.log_event(
        event_type="pipeline_start", message="Continuous ingestion started (NYISO + weather)"
    )

    nyiso_pipeline = build_continuous_pipeline(
        async_store, poll_interval=float(poll_interval_seconds)
    )
    weather_pipeline = build_weather_continuous_pipeline(
        async_store, poll_interval=float(weather_poll_interval_seconds)
    )

    logger.info("NYISO continuous pipeline topology:\n%s", nyiso_pipeline.topology)
    logger.info("Weather continuous pipeline topology:\n%s", weather_pipeline.topology)

    nyiso_result, weather_result = await asyncio.gather(
        nyiso_pipeline.run(),
        weather_pipeline.run(),
    )

    await async_store.log_event(
        event_type="pipeline_stop",
        message=(
            f"Ingestion stopped — NYISO: {nyiso_result.duration_seconds:.1f}s, "
            f"Weather: {weather_result.duration_seconds:.1f}s"
        ),
    )
    logger.info("NYISO ingestion stopped:\n%s", nyiso_result.summary())
    logger.info("Weather ingestion stopped:\n%s", weather_result.summary())

    return nyiso_result, weather_result
