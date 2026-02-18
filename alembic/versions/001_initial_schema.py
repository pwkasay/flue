"""Initial schema — fuel_mix, carbon_intensity, weather, ingestion_events.

Revision ID: 001
Revises: None
Create Date: 2026-02-18

Raw SQL migrations — no SQLAlchemy models, matching the project's
"raw SQL, no ORM" philosophy.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE fuel_mix (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            fuel_category TEXT NOT NULL,
            generation_mw DOUBLE PRECISION NOT NULL,
            UNIQUE(timestamp, fuel_category)
        )
    """)
    op.execute("CREATE INDEX idx_fm_timestamp ON fuel_mix(timestamp)")

    op.execute("""
        CREATE TABLE carbon_intensity (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL UNIQUE,
            grams_co2_per_kwh DOUBLE PRECISION NOT NULL,
            total_generation_mw DOUBLE PRECISION NOT NULL,
            clean_percentage DOUBLE PRECISION NOT NULL,
            fuel_breakdown_json JSONB
        )
    """)
    op.execute("CREATE INDEX idx_ci_timestamp ON carbon_intensity(timestamp)")

    op.execute("""
        CREATE TABLE weather (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL UNIQUE,
            temperature_f DOUBLE PRECISION,
            wind_speed_80m_mph DOUBLE PRECISION,
            cloud_cover_pct DOUBLE PRECISION
        )
    """)
    op.execute("CREATE INDEX idx_weather_timestamp ON weather(timestamp)")

    op.execute("""
        CREATE TABLE ingestion_events (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            event_type TEXT NOT NULL,
            stage_name TEXT,
            message TEXT,
            details_json JSONB
        )
    """)
    op.execute("CREATE INDEX idx_ie_timestamp ON ingestion_events(timestamp)")
    op.execute("CREATE INDEX idx_ie_event_type ON ingestion_events(event_type)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ingestion_events")
    op.execute("DROP TABLE IF EXISTS weather")
    op.execute("DROP TABLE IF EXISTS carbon_intensity")
    op.execute("DROP TABLE IF EXISTS fuel_mix")
