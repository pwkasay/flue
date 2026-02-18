"""NYISO data source adapter.

Fetches real-time fuel mix data from NYISO's public data site.
No authentication required. Data is available as CSV files at predictable URLs:

  http://mis.nyiso.com/public/csv/rtfuelmix/{YYYYMMDD}rtfuelmix.csv

Each CSV has columns: Time Stamp, Time Zone, Fuel Category, Gen MW
Updated every 5 minutes. Historical data available back to ~2013.

This module provides both sync (for CLI/seeding) and async (for pipeline)
interfaces to the same data.
"""


import io
import csv
import logging
from datetime import date, datetime, timedelta, timezone
from typing import AsyncIterator
from zoneinfo import ZoneInfo

import httpx

from ..models.fuel_mix import FuelGeneration, FuelMix
from ..models.exceptions import NYISOFetchError
from ..sources.emission_factors import NYISOFuelCategory

logger = logging.getLogger("gridcarbon.sources.nyiso")

NYISO_BASE_URL = "http://mis.nyiso.com/public/csv/rtfuelmix"
EASTERN = ZoneInfo("America/New_York")


def _build_url(day: date) -> str:
    """Build the NYISO fuel mix CSV URL for a given date."""
    return f"{NYISO_BASE_URL}/{day.strftime('%Y%m%d')}rtfuelmix.csv"


def _parse_csv(text: str, source_date: date) -> list[FuelMix]:
    """Parse NYISO fuel mix CSV text into FuelMix objects.

    The CSV has one row per (timestamp, fuel_category) combination.
    We group rows by timestamp to assemble complete FuelMix snapshots.
    """
    reader = csv.DictReader(io.StringIO(text))

    # Group rows by timestamp
    by_timestamp: dict[str, list[FuelGeneration]] = {}
    for row in reader:
        ts_str = row.get("Time Stamp", "").strip()
        fuel_label = row.get("Fuel Category", "").strip()
        gen_str = row.get("Gen MW", "0").strip()

        if not ts_str or not fuel_label:
            continue

        try:
            fuel = NYISOFuelCategory.from_nyiso_label(fuel_label)
            gen_mw = float(gen_str)
        except (ValueError, Exception) as e:
            logger.debug("Skipping row: %s (%s)", row, e)
            continue

        if ts_str not in by_timestamp:
            by_timestamp[ts_str] = []
        by_timestamp[ts_str].append(FuelGeneration(fuel=fuel, generation_mw=gen_mw))

    # Convert to FuelMix objects
    mixes = []
    for ts_str, fuels in sorted(by_timestamp.items()):
        try:
            # NYISO timestamps are like "01/15/2024 00:05:00"
            ts = datetime.strptime(ts_str, "%m/%d/%Y %H:%M:%S")
            ts = ts.replace(tzinfo=EASTERN)
        except ValueError:
            logger.debug("Could not parse timestamp: %s", ts_str)
            continue

        mixes.append(FuelMix(timestamp=ts, fuels=fuels))

    return mixes


async def fetch_fuel_mix_async(
    day: date,
    client: httpx.AsyncClient | None = None,
) -> list[FuelMix]:
    """Fetch fuel mix data for a single day (async).

    Returns a list of FuelMix snapshots (up to 288 per day at 5-min intervals).
    """
    url = _build_url(day)
    logger.info("Fetching NYISO fuel mix for %s", day.isoformat())

    should_close = client is None
    client = client or httpx.AsyncClient(timeout=30.0)

    try:
        resp = await client.get(url)
        resp.raise_for_status()
        return _parse_csv(resp.text, day)
    except httpx.HTTPStatusError as e:
        raise NYISOFetchError(
            f"NYISO returned {e.response.status_code} for {url}"
        ) from e
    except httpx.RequestError as e:
        raise NYISOFetchError(f"Failed to fetch {url}: {e}") from e
    finally:
        if should_close:
            await client.aclose()


def fetch_fuel_mix_sync(day: date) -> list[FuelMix]:
    """Fetch fuel mix data for a single day (sync, for CLI/seeding)."""
    url = _build_url(day)
    logger.info("Fetching NYISO fuel mix for %s", day.isoformat())

    try:
        resp = httpx.get(url, timeout=30.0)
        resp.raise_for_status()
        return _parse_csv(resp.text, day)
    except httpx.HTTPStatusError as e:
        raise NYISOFetchError(
            f"NYISO returned {e.response.status_code} for {url}"
        ) from e
    except httpx.RequestError as e:
        raise NYISOFetchError(f"Failed to fetch {url}: {e}") from e


async def fetch_fuel_mix_range(
    start: date,
    end: date,
) -> AsyncIterator[FuelMix]:
    """Fetch fuel mix data for a date range (async generator).

    Yields individual FuelMix snapshots across all days in the range.
    Suitable as an asyncpipe source.
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        current = start
        while current <= end:
            try:
                mixes = await fetch_fuel_mix_async(current, client=client)
                for mix in mixes:
                    yield mix
            except NYISOFetchError as e:
                logger.warning("Failed to fetch %s: %s", current, e)
            current += timedelta(days=1)


async def fetch_latest() -> FuelMix | None:
    """Fetch the most recent fuel mix snapshot.

    Tries today first, then yesterday (in case it's just after midnight
    and today's data isn't posted yet).
    """
    today = date.today()
    for day in [today, today - timedelta(days=1)]:
        try:
            mixes = await fetch_fuel_mix_async(day)
            if mixes:
                return mixes[-1]  # Most recent
        except NYISOFetchError:
            continue
    return None
