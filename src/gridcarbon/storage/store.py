"""PostgreSQL storage for historical fuel mix, carbon intensity, and weather data.

Sync interface using psycopg3. Used by CLI commands and the HeuristicForecaster.
For async pipeline/API usage, see async_store.py.

No ORM — just psycopg with clear SQL. Schema is managed by Alembic migrations.
"""

import json
import logging
import os
from datetime import date, datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row

from ..models.fuel_mix import FuelMix
from ..models.exceptions import StoreError

logger = logging.getLogger("gridcarbon.storage")

DEFAULT_DSN = "postgresql://gridcarbon:gridcarbon@localhost:5432/gridcarbon"


class Store:
    """PostgreSQL-backed historical data store (sync, psycopg3).

    Usage:
        store = Store()  # Reads DATABASE_URL from env
        store.save_fuel_mix(fuel_mix)
        recent = store.get_carbon_intensity(hours=24)
    """

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or os.environ.get("DATABASE_URL", DEFAULT_DSN)
        self._conn = psycopg.connect(self.dsn, row_factory=dict_row)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # ── Write ──

    def save_fuel_mix(self, mix: FuelMix) -> None:
        """Save a FuelMix snapshot (fuel breakdown + computed carbon intensity)."""
        ts = mix.timestamp

        try:
            with self._conn.transaction():
                for fuel in mix.fuels:
                    self._conn.execute(
                        """INSERT INTO fuel_mix (timestamp, fuel_category, generation_mw)
                           VALUES (%s, %s, %s)
                           ON CONFLICT (timestamp, fuel_category)
                           DO UPDATE SET generation_mw = EXCLUDED.generation_mw""",
                        (ts, fuel.fuel.value, fuel.generation_mw),
                    )

                ci = mix.carbon_intensity
                self._conn.execute(
                    """INSERT INTO carbon_intensity
                       (timestamp, grams_co2_per_kwh, total_generation_mw,
                        clean_percentage, fuel_breakdown_json)
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT (timestamp)
                       DO UPDATE SET grams_co2_per_kwh = EXCLUDED.grams_co2_per_kwh,
                                     total_generation_mw = EXCLUDED.total_generation_mw,
                                     clean_percentage = EXCLUDED.clean_percentage,
                                     fuel_breakdown_json = EXCLUDED.fuel_breakdown_json""",
                    (
                        ts,
                        ci.grams_co2_per_kwh,
                        mix.total_generation_mw,
                        mix.clean_percentage,
                        json.dumps(mix.fuel_breakdown),
                    ),
                )
        except psycopg.Error as e:
            raise StoreError(f"Failed to save fuel mix: {e}") from e

    def save_fuel_mixes(self, mixes: list[FuelMix]) -> int:
        """Bulk save fuel mix snapshots. Returns count saved."""
        count = 0
        for mix in mixes:
            try:
                self.save_fuel_mix(mix)
                count += 1
            except StoreError as e:
                logger.warning("Skipping fuel mix save: %s", e)
        return count

    def save_weather(
        self, timestamp: datetime, temp_f: float, wind_mph: float, cloud_pct: float
    ) -> None:
        """Save a weather observation."""
        try:
            with self._conn.transaction():
                self._conn.execute(
                    """INSERT INTO weather
                       (timestamp, temperature_f, wind_speed_80m_mph, cloud_cover_pct)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (timestamp)
                       DO UPDATE SET temperature_f = EXCLUDED.temperature_f,
                                     wind_speed_80m_mph = EXCLUDED.wind_speed_80m_mph,
                                     cloud_cover_pct = EXCLUDED.cloud_cover_pct""",
                    (timestamp, temp_f, wind_mph, cloud_pct),
                )
        except psycopg.Error as e:
            raise StoreError(f"Failed to save weather: {e}") from e

    # ── Read ──

    def get_carbon_intensity(self, hours: int = 24) -> list[dict[str, Any]]:
        """Get recent carbon intensity data points."""
        rows = self._conn.execute(
            """SELECT timestamp, grams_co2_per_kwh, total_generation_mw,
                      clean_percentage, fuel_breakdown_json
               FROM carbon_intensity
               WHERE timestamp > NOW() - make_interval(hours => %s)
               ORDER BY timestamp ASC""",
            (hours,),
        ).fetchall()

        return [
            {
                "timestamp": row["timestamp"].isoformat(),
                "grams_co2_per_kwh": row["grams_co2_per_kwh"],
                "total_generation_mw": row["total_generation_mw"],
                "clean_percentage": row["clean_percentage"],
                "fuel_breakdown": row["fuel_breakdown_json"] or {},
            }
            for row in rows
        ]

    def get_latest_intensity(self) -> dict[str, Any] | None:
        """Get the most recent carbon intensity record."""
        row = self._conn.execute(
            """SELECT timestamp, grams_co2_per_kwh, total_generation_mw,
                      clean_percentage, fuel_breakdown_json
               FROM carbon_intensity
               ORDER BY timestamp DESC LIMIT 1""",
        ).fetchone()

        if row is None:
            return None

        return {
            "timestamp": row["timestamp"].isoformat(),
            "grams_co2_per_kwh": row["grams_co2_per_kwh"],
            "total_generation_mw": row["total_generation_mw"],
            "clean_percentage": row["clean_percentage"],
            "fuel_breakdown": row["fuel_breakdown_json"] or {},
        }

    def get_hourly_averages(
        self, month: int | None = None, day_of_week: int | None = None
    ) -> dict[int, float]:
        """Get average carbon intensity by hour of day.

        Used by the heuristic forecaster to build the baseline lookup table.

        Args:
            month: Filter to a specific month (1-12). None = all months.
            day_of_week: Filter to a specific day (0=Mon, 6=Sun). None = all days.

        Returns:
            Dict mapping hour (0-23) -> average gCO2/kWh.
        """
        conditions = []
        params: list[Any] = []

        if month is not None:
            conditions.append("EXTRACT(MONTH FROM timestamp) = %s")
            params.append(month)
        if day_of_week is not None:
            # PostgreSQL: EXTRACT(DOW) returns 0=Sunday, 1=Monday, ...
            # We want 0=Monday, so adjust
            pg_dow = (day_of_week + 1) % 7
            conditions.append("EXTRACT(DOW FROM timestamp) = %s")
            params.append(pg_dow)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        rows = self._conn.execute(
            f"""SELECT EXTRACT(HOUR FROM timestamp)::int AS hour,
                       AVG(grams_co2_per_kwh) AS avg_ci
                FROM carbon_intensity
                {where}
                GROUP BY hour
                ORDER BY hour""",
            params,
        ).fetchall()

        return {row["hour"]: row["avg_ci"] for row in rows}

    def get_intensity_range(self, start: date, end: date) -> list[dict[str, Any]]:
        """Get carbon intensity data for a date range."""
        rows = self._conn.execute(
            """SELECT timestamp, grams_co2_per_kwh, total_generation_mw,
                      clean_percentage
               FROM carbon_intensity
               WHERE timestamp >= %s AND timestamp < %s
               ORDER BY timestamp ASC""",
            (start, end),
        ).fetchall()

        return [
            {
                "timestamp": row["timestamp"].isoformat(),
                "grams_co2_per_kwh": row["grams_co2_per_kwh"],
                "total_generation_mw": row["total_generation_mw"],
                "clean_percentage": row["clean_percentage"],
            }
            for row in rows
        ]

    def record_count(self) -> int:
        """Total carbon intensity records stored."""
        row = self._conn.execute("SELECT COUNT(*) AS cnt FROM carbon_intensity").fetchone()
        return row["cnt"] if row else 0

    def date_range(self) -> tuple[str | None, str | None]:
        """Return the earliest and latest timestamps in the store."""
        row = self._conn.execute(
            "SELECT MIN(timestamp) AS earliest, MAX(timestamp) AS latest FROM carbon_intensity"
        ).fetchone()
        if row and row["earliest"]:
            return row["earliest"].isoformat(), row["latest"].isoformat()
        return None, None

    # ── Ingestion Events ──

    def log_event(
        self,
        event_type: str,
        stage_name: str | None = None,
        message: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Record an ingestion event for admin visibility."""
        try:
            with self._conn.transaction():
                self._conn.execute(
                    """INSERT INTO ingestion_events
                       (event_type, stage_name, message, details_json)
                       VALUES (%s, %s, %s, %s)""",
                    (event_type, stage_name, message, json.dumps(details) if details else None),
                )
        except psycopg.Error as e:
            logger.warning("Failed to log event: %s", e)

    def get_recent_events(
        self, limit: int = 50, event_type: str | None = None
    ) -> list[dict[str, Any]]:
        """Query recent ingestion events."""
        if event_type:
            rows = self._conn.execute(
                """SELECT timestamp, event_type, stage_name, message, details_json
                   FROM ingestion_events
                   WHERE event_type = %s
                   ORDER BY timestamp DESC LIMIT %s""",
                (event_type, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """SELECT timestamp, event_type, stage_name, message, details_json
                   FROM ingestion_events
                   ORDER BY timestamp DESC LIMIT %s""",
                (limit,),
            ).fetchall()

        return [
            {
                "timestamp": row["timestamp"].isoformat(),
                "event_type": row["event_type"],
                "stage_name": row["stage_name"],
                "message": row["message"],
                "details": row["details_json"],
            }
            for row in rows
        ]

    def get_weather_freshness(self) -> dict[str, Any]:
        """Query weather table freshness for admin status."""
        row = self._conn.execute(
            """SELECT
                 MAX(timestamp) AS latest,
                 (SELECT COUNT(*) FROM weather
                  WHERE timestamp > NOW() - INTERVAL '1 hour') AS records_last_hour
               FROM weather"""
        ).fetchone()

        latest = row["latest"] if row else None
        records_last_hour = row["records_last_hour"] if row else 0

        if latest:
            age = datetime.now(timezone.utc) - latest.astimezone(timezone.utc)
            if age.total_seconds() < 7200:
                weather_status = "active"
            elif age.total_seconds() < 86400:
                weather_status = "stale"
            else:
                weather_status = "inactive"
        else:
            weather_status = "inactive"

        return {
            "status": weather_status,
            "last_data_at": latest.isoformat() if latest else None,
            "records_last_hour": records_last_hour,
            "provider": "Open-Meteo",
        }

    def get_ingestion_status(self) -> dict[str, Any]:
        """Derived ingestion status for the admin dashboard."""
        count = self.record_count()
        earliest, latest = self.date_range()

        # Records and errors in the last hour
        stats = self._conn.execute(
            """SELECT
                 (SELECT COUNT(*) FROM carbon_intensity
                  WHERE timestamp > NOW() - INTERVAL '1 hour') AS records_last_hour,
                 (SELECT COUNT(*) FROM ingestion_events
                  WHERE event_type IN ('validation_failure', 'persist_failure')
                  AND timestamp > NOW() - INTERVAL '1 hour') AS errors_last_hour"""
        ).fetchone()

        # Determine activity status based on latest record
        is_active = False
        if latest:
            latest_dt = datetime.fromisoformat(latest)
            age = datetime.now(timezone.utc) - latest_dt.astimezone(timezone.utc)
            is_active = age.total_seconds() < 600  # Active if data < 10 min old

        return {
            "is_active": is_active,
            "last_data_at": latest,
            "records_last_hour": stats["records_last_hour"] if stats else 0,
            "errors_last_hour": stats["errors_last_hour"] if stats else 0,
            "total_records": count,
            "earliest": earliest,
            "latest": latest,
        }
