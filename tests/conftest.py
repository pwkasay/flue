"""Shared test fixtures for gridcarbon.

Provides Postgres-backed Store and AsyncStore instances with transaction
rollback for test isolation. Tests require a running Postgres instance.

Set TEST_DATABASE_URL to override the default connection string.
"""

import os

import psycopg
import pytest
import pytest_asyncio

TEST_DSN = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://gridcarbon:gridcarbon@localhost:5432/gridcarbon_test",
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fuel_mix (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    fuel_category TEXT NOT NULL,
    generation_mw DOUBLE PRECISION NOT NULL,
    UNIQUE(timestamp, fuel_category)
);
CREATE TABLE IF NOT EXISTS carbon_intensity (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL UNIQUE,
    grams_co2_per_kwh DOUBLE PRECISION NOT NULL,
    total_generation_mw DOUBLE PRECISION NOT NULL,
    clean_percentage DOUBLE PRECISION NOT NULL,
    fuel_breakdown_json JSONB
);
CREATE TABLE IF NOT EXISTS weather (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL UNIQUE,
    temperature_f DOUBLE PRECISION,
    wind_speed_80m_mph DOUBLE PRECISION,
    cloud_cover_pct DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS ingestion_events (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_type TEXT NOT NULL,
    stage_name TEXT,
    message TEXT,
    details_json JSONB
);
CREATE INDEX IF NOT EXISTS idx_fm_timestamp ON fuel_mix(timestamp);
CREATE INDEX IF NOT EXISTS idx_ci_timestamp ON carbon_intensity(timestamp);
CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather(timestamp);
CREATE INDEX IF NOT EXISTS idx_ie_timestamp ON ingestion_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_ie_event_type ON ingestion_events(event_type);
"""


def _pg_available() -> bool:
    """Check if test Postgres is reachable."""
    try:
        conn = psycopg.connect(TEST_DSN, connect_timeout=3)
        conn.close()
        return True
    except Exception:
        return False


requires_postgres = pytest.mark.skipif(
    not _pg_available(),
    reason=f"Postgres not available at {TEST_DSN}",
)


def _ensure_schema() -> None:
    """Create tables if they don't exist (idempotent)."""
    conn = psycopg.connect(TEST_DSN, autocommit=True)
    conn.execute(SCHEMA_SQL)
    conn.close()


def _truncate_tables() -> None:
    """Truncate all tables for a clean test run."""
    conn = psycopg.connect(TEST_DSN, autocommit=True)
    conn.execute(
        "TRUNCATE fuel_mix, carbon_intensity, weather, ingestion_events RESTART IDENTITY CASCADE"
    )
    conn.close()


@pytest.fixture
def sync_store():
    """Sync Store fixture — truncates tables before each test."""
    from gridcarbon.storage.store import Store

    _ensure_schema()
    _truncate_tables()
    store = Store(dsn=TEST_DSN)
    yield store
    store.close()


@pytest_asyncio.fixture
async def async_store():
    """Async Store fixture — truncates tables before each test."""
    from gridcarbon.storage.async_store import AsyncStore

    _ensure_schema()
    _truncate_tables()
    store = await AsyncStore.create(dsn=TEST_DSN)
    yield store
    await store.close()
