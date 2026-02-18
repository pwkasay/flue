"""Ingestion pipeline built on asyncpipe (Sluice).

This is where the two portfolio projects meet. The pipeline uses asyncpipe's
stage decorator, error routing, backpressure, and metrics collection to
ingest NYISO fuel mix data.

Two pipeline configurations:
1. Seed pipeline — batch historical import across a date range
2. Continuous pipeline — poll every 5 minutes for latest data

Both use the same stages (validate, persist) wired through asyncpipe's
Pipeline builder. The source is an async generator in both cases — the
pipeline handles everything downstream.

Architecture:
    source (async gen, yields FuelMix)
      → validate  (check data quality, route bad records to dead letters)
      → persist   (SQLite write, concurrency=1 for single-writer safety)
      → [dead letter collector catches validation failures]

Error strategy:
    - ValidationError → dead letter collector (logged, counted, inspectable)
    - NYISOFetchError → logged in source, skipped (source-level resilience)
    - StoreError → retried once (transient SQLite lock), then dead-lettered
"""


import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Any, AsyncIterator

import httpx

from asyncpipe import Pipeline, PipelineResult, stage

from ..models.fuel_mix import FuelMix
from ..models.exceptions import (
    GridCarbonException,
    NYISOFetchError,
    StoreError,
)
from ..sources.nyiso import fetch_fuel_mix_async, fetch_latest
from ..storage.store import Store

logger = logging.getLogger("gridcarbon.pipeline")


# ─── Exceptions (pipeline-specific) ───


class ValidationError(GridCarbonException):
    """A FuelMix record failed validation."""
    pass


# ─── Sources (async generators) ───


async def nyiso_date_source(
    start: date,
    end: date,
    rate_limit_delay: float = 0.5,
    progress_callback: Any | None = None,
) -> AsyncIterator[FuelMix]:
    """Async generator that yields individual FuelMix objects for a date range.

    This is the asyncpipe source for the seed pipeline. It handles:
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

                logger.debug(
                    "Source yielded %d records for %s", len(mixes), current.isoformat()
                )

            except NYISOFetchError as e:
                logger.warning("Source skipping %s: %s", current.isoformat(), e)

            # Rate limit — be polite to NYISO
            await asyncio.sleep(rate_limit_delay)
            current += timedelta(days=1)

        logger.info("Source exhausted: %d days fetched", days_fetched)


async def continuous_source(
    poll_interval: float = 300.0,
) -> AsyncIterator[FuelMix]:
    """Infinite async generator that polls NYISO for the latest fuel mix.

    This is the asyncpipe source for continuous ingestion.
    Yields one FuelMix every poll_interval seconds.
    Runs until the pipeline is shut down (Ctrl+C triggers asyncpipe's
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
            f"Only {len(mix.fuels)} fuel categories at {mix.timestamp.isoformat()} "
            f"(expected ≥3)"
        )

    for fuel in mix.fuels:
        if fuel.generation_mw < 0:
            raise ValidationError(
                f"Negative generation ({fuel.generation_mw} MW) for "
                f"{fuel.fuel.value} at {mix.timestamp.isoformat()}"
            )

    return mix


def make_persist_stage(store: Store):
    """Factory for the persist stage — closes over the Store instance.

    Why a factory? The @stage decorator freezes the function at decoration
    time, but the Store is created at runtime (its path might come from
    CLI args or env vars). So we create the decorated stage dynamically.
    """

    @stage(concurrency=1, retries=2, retry_base_delay=0.1)
    async def persist(mix: FuelMix) -> FuelMix:
        """Persist a validated FuelMix to the SQLite store.

        concurrency=1: SQLite is single-writer. Asyncpipe's bounded channels
        provide backpressure so upstream stages don't overwhelm this bottleneck.

        retries=2: Transient SQLite "database is locked" errors are retried
        once with a short delay. Persistent failures go to dead letters.
        """
        try:
            store.save_fuel_mix(mix)
        except StoreError:
            raise  # Let asyncpipe's retry machinery handle it
        except Exception as e:
            raise StoreError(f"Unexpected persist error: {e}") from e
        return mix

    return persist


# ─── Pipeline Builders ───


def build_seed_pipeline(
    store: Store,
    start: date,
    end: date,
    channel_capacity: int = 128,
    progress_callback: Any | None = None,
) -> Pipeline:
    """Build the historical seed pipeline.

    Returns a built (but not yet running) Pipeline.

    Architecture:
        nyiso_date_source → validate → persist
        ValidationError ─→ dead letters
        StoreError ──────→ dead letters (after 1 retry)
    """
    return (
        Pipeline(
            "gridcarbon-seed",
            channel_capacity=channel_capacity,
            drain_timeout=60.0,
            log_level=logging.INFO,
        )
        .source(nyiso_date_source(start, end, progress_callback=progress_callback))
        .then(validate)
        .then(make_persist_stage(store))
        .on_error(ValidationError)    # → dead letter collector (default handler)
        .on_error(StoreError)         # → dead letter collector (after retries)
        .build()
    )


def build_continuous_pipeline(
    store: Store,
    poll_interval: float = 300.0,
    channel_capacity: int = 16,
) -> Pipeline:
    """Build the continuous ingestion pipeline.

    Runs until Ctrl+C. asyncpipe installs signal handlers for graceful
    shutdown: the source stops yielding, in-flight items drain through
    the stages, and the pipeline returns a result summary.

    Architecture:
        continuous_source → validate → persist
    """
    return (
        Pipeline(
            "gridcarbon-ingest",
            channel_capacity=channel_capacity,
            drain_timeout=15.0,
            log_level=logging.INFO,
        )
        .source(continuous_source(poll_interval=poll_interval))
        .then(validate)
        .then(make_persist_stage(store))
        .on_error(ValidationError)
        .on_error(StoreError)
        .build()
    )


# ─── Runner Functions (called by CLI) ───


async def run_seed(
    store: Store,
    start: date,
    end: date,
    progress_callback: Any | None = None,
) -> PipelineResult:
    """Seed historical data using the asyncpipe pipeline.

    Returns PipelineResult with metrics: items processed per stage,
    latency percentiles, error counts, dead letters, duration.
    """
    pipeline = build_seed_pipeline(
        store, start, end, progress_callback=progress_callback
    )

    logger.info("Seed pipeline topology:\n%s", pipeline.topology)
    result = await pipeline.run()
    logger.info("Seed complete:\n%s", result.summary())

    return result


async def run_continuous(
    store: Store,
    poll_interval_seconds: int = 300,
    **kwargs: Any,
) -> PipelineResult:
    """Run continuous ingestion using the asyncpipe pipeline.

    Runs until interrupted (Ctrl+C). Returns PipelineResult on shutdown.
    """
    pipeline = build_continuous_pipeline(
        store, poll_interval=float(poll_interval_seconds)
    )

    logger.info("Continuous pipeline topology:\n%s", pipeline.topology)
    result = await pipeline.run()
    logger.info("Ingestion stopped:\n%s", result.summary())

    return result
