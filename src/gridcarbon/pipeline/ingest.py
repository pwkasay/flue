"""Ingestion pipeline built on weir (Sluice).

This is where the two portfolio projects meet. The pipeline uses weir's
stage decorator, error routing, backpressure, and metrics collection to
ingest NYISO fuel mix data.

Two pipeline configurations:
1. Seed pipeline — batch historical import across a date range
2. Continuous pipeline — poll every 5 minutes for latest data

Both use the same stages (validate, persist) wired through weir's
Pipeline builder. The source is an async generator in both cases — the
pipeline handles everything downstream.

Architecture:
    source (async gen, yields FuelMix)
      → validate  (check data quality, route bad records to dead letters)
      → persist   (Postgres write via AsyncStore, concurrency=1)
      → [dead letter collector catches validation failures]
      → [event logging handler records failures to ingestion_events]

Error strategy:
    - ValidationError → event logged + dead lettered
    - NYISOFetchError → logged in source, skipped (source-level resilience)
    - StoreError → retried once, then event logged + dead-lettered
"""

import asyncio
import logging
from datetime import date, timedelta
from typing import Any, AsyncIterator

import httpx

from weir import FailedItem, Pipeline, PipelineResult, stage

from ..models.fuel_mix import FuelMix
from ..models.exceptions import (
    GridCarbonException,
    NYISOFetchError,
    StoreError,
)
from ..sources.nyiso import fetch_fuel_mix_async, fetch_latest
from ..storage.async_store import AsyncStore

logger = logging.getLogger("gridcarbon.pipeline")


# ─── Exceptions (pipeline-specific) ───


class ValidationError(GridCarbonException):
    """A FuelMix record failed validation."""

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


# ─── Sources (async generators) ───


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


# ─── Pipeline Stages ───
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


# ─── Pipeline Builders ───


def build_seed_pipeline(
    async_store: AsyncStore,
    start: date,
    end: date,
    channel_capacity: int = 128,
    progress_callback: Any | None = None,
) -> Pipeline:
    """Build the historical seed pipeline.

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
        .build()
    )


def build_continuous_pipeline(
    async_store: AsyncStore,
    poll_interval: float = 300.0,
    channel_capacity: int = 16,
) -> Pipeline:
    """Build the continuous ingestion pipeline.

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
        .build()
    )


# ─── Runner Functions (called by CLI) ───


async def run_seed(
    async_store: AsyncStore,
    start: date,
    end: date,
    progress_callback: Any | None = None,
) -> PipelineResult:
    """Seed historical data using the weir pipeline.

    Returns PipelineResult with metrics: items processed per stage,
    latency percentiles, error counts, dead letters, duration.
    """
    pipeline = build_seed_pipeline(async_store, start, end, progress_callback=progress_callback)

    logger.info("Seed pipeline topology:\n%s", pipeline.topology)
    result = await pipeline.run()
    logger.info("Seed complete:\n%s", result.summary())

    return result


async def run_continuous(
    async_store: AsyncStore,
    poll_interval_seconds: int = 300,
    **kwargs: Any,
) -> PipelineResult:
    """Run continuous ingestion using the weir pipeline.

    Runs until interrupted (Ctrl+C). Returns PipelineResult on shutdown.
    """
    await async_store.log_event(event_type="pipeline_start", message="Continuous ingestion started")
    pipeline = build_continuous_pipeline(async_store, poll_interval=float(poll_interval_seconds))

    logger.info("Continuous pipeline topology:\n%s", pipeline.topology)
    result = await pipeline.run()
    await async_store.log_event(
        event_type="pipeline_stop",
        message=f"Ingestion stopped after {result.duration_seconds:.1f}s",
    )
    logger.info("Ingestion stopped:\n%s", result.summary())

    return result
