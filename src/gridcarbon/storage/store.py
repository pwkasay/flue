"""SQLite storage for historical fuel mix, carbon intensity, and weather data.

Deliberately simple. No ORM — just sqlite3 with clear SQL.
The schema is designed for the two primary query patterns:

1. Time-series retrieval: "Give me carbon intensity for the last 24 hours"
2. Heuristic lookups: "What's the average CI for Tuesdays at 3pm in January?"
"""


import json
import logging
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from ..models.fuel_mix import CarbonIntensity, FuelGeneration, FuelMix
from ..models.exceptions import StoreError
from ..sources.emission_factors import NYISOFuelCategory

logger = logging.getLogger("gridcarbon.storage")

DEFAULT_DB_PATH = Path.home() / ".gridcarbon" / "gridcarbon.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS fuel_mix (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    fuel_category TEXT NOT NULL,
    generation_mw REAL NOT NULL,
    UNIQUE(timestamp, fuel_category)
);

CREATE TABLE IF NOT EXISTS carbon_intensity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL UNIQUE,
    grams_co2_per_kwh REAL NOT NULL,
    total_generation_mw REAL NOT NULL,
    clean_percentage REAL NOT NULL,
    fuel_breakdown_json TEXT
);

CREATE TABLE IF NOT EXISTS weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL UNIQUE,
    temperature_f REAL,
    wind_speed_80m_mph REAL,
    cloud_cover_pct REAL
);

CREATE INDEX IF NOT EXISTS idx_ci_timestamp ON carbon_intensity(timestamp);
CREATE INDEX IF NOT EXISTS idx_fm_timestamp ON fuel_mix(timestamp);
CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather(timestamp);
"""


class Store:
    """SQLite-backed historical data store.

    Usage:
        store = Store()  # Uses default path ~/.gridcarbon/gridcarbon.db
        store.save_fuel_mix(fuel_mix)
        recent = store.get_carbon_intensity(hours=24)
    """

    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self.db_path),
            detect_types=sqlite3.PARSE_DECLTYPES,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Store:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # ── Write ──

    def save_fuel_mix(self, mix: FuelMix) -> None:
        """Save a FuelMix snapshot (fuel breakdown + computed carbon intensity)."""
        ts = mix.timestamp.isoformat()

        try:
            with self._conn:
                # Save individual fuel rows
                for fuel in mix.fuels:
                    self._conn.execute(
                        """INSERT OR REPLACE INTO fuel_mix
                           (timestamp, fuel_category, generation_mw)
                           VALUES (?, ?, ?)""",
                        (ts, fuel.fuel.value, fuel.generation_mw),
                    )

                # Save computed carbon intensity
                ci = mix.carbon_intensity
                self._conn.execute(
                    """INSERT OR REPLACE INTO carbon_intensity
                       (timestamp, grams_co2_per_kwh, total_generation_mw,
                        clean_percentage, fuel_breakdown_json)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        ts,
                        ci.grams_co2_per_kwh,
                        mix.total_generation_mw,
                        mix.clean_percentage,
                        json.dumps(mix.fuel_breakdown),
                    ),
                )
        except sqlite3.Error as e:
            raise StoreError(f"Failed to save fuel mix: {e}") from e

    def save_fuel_mixes(self, mixes: list[FuelMix]) -> int:
        """Bulk save fuel mix snapshots. Returns count saved."""
        count = 0
        for mix in mixes:
            try:
                self.save_fuel_mix(mix)
                count += 1
            except StoreError:
                pass  # Skip duplicates
        return count

    def save_weather(
        self, timestamp: datetime, temp_f: float, wind_mph: float, cloud_pct: float
    ) -> None:
        """Save a weather observation."""
        try:
            with self._conn:
                self._conn.execute(
                    """INSERT OR REPLACE INTO weather
                       (timestamp, temperature_f, wind_speed_80m_mph, cloud_cover_pct)
                       VALUES (?, ?, ?, ?)""",
                    (timestamp.isoformat(), temp_f, wind_mph, cloud_pct),
                )
        except sqlite3.Error as e:
            raise StoreError(f"Failed to save weather: {e}") from e

    # ── Read ──

    def get_carbon_intensity(
        self, hours: int = 24
    ) -> list[dict[str, Any]]:
        """Get recent carbon intensity data points."""
        cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
        rows = self._conn.execute(
            """SELECT timestamp, grams_co2_per_kwh, total_generation_mw,
                      clean_percentage, fuel_breakdown_json
               FROM carbon_intensity
               WHERE timestamp > ?
               ORDER BY timestamp ASC""",
            (cutoff,),
        ).fetchall()

        return [
            {
                "timestamp": row["timestamp"],
                "grams_co2_per_kwh": row["grams_co2_per_kwh"],
                "total_generation_mw": row["total_generation_mw"],
                "clean_percentage": row["clean_percentage"],
                "fuel_breakdown": json.loads(row["fuel_breakdown_json"])
                if row["fuel_breakdown_json"]
                else {},
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
            "timestamp": row["timestamp"],
            "grams_co2_per_kwh": row["grams_co2_per_kwh"],
            "total_generation_mw": row["total_generation_mw"],
            "clean_percentage": row["clean_percentage"],
            "fuel_breakdown": json.loads(row["fuel_breakdown_json"])
            if row["fuel_breakdown_json"]
            else {},
        }

    def get_hourly_averages(
        self, month: int | None = None, day_of_week: int | None = None
    ) -> dict[int, float]:
        """Get average carbon intensity by hour of day.

        Used by the heuristic forecaster to build the baseline lookup table.

        Args:
            month: Filter to a specific month (1–12). None = all months.
            day_of_week: Filter to a specific day (0=Mon, 6=Sun). None = all days.

        Returns:
            Dict mapping hour (0–23) → average gCO₂/kWh.
        """
        conditions = []
        params: list[Any] = []

        if month is not None:
            conditions.append("CAST(strftime('%m', timestamp) AS INTEGER) = ?")
            params.append(month)
        if day_of_week is not None:
            # SQLite: strftime('%w') returns 0=Sunday, 1=Monday, ...
            # We want 0=Monday, so adjust
            sqlite_dow = (day_of_week + 1) % 7
            conditions.append("CAST(strftime('%w', timestamp) AS INTEGER) = ?")
            params.append(sqlite_dow)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        rows = self._conn.execute(
            f"""SELECT CAST(strftime('%H', timestamp) AS INTEGER) as hour,
                       AVG(grams_co2_per_kwh) as avg_ci
                FROM carbon_intensity
                {where}
                GROUP BY hour
                ORDER BY hour""",
            params,
        ).fetchall()

        return {row["hour"]: row["avg_ci"] for row in rows}

    def get_intensity_range(
        self, start: date, end: date
    ) -> list[dict[str, Any]]:
        """Get carbon intensity data for a date range."""
        rows = self._conn.execute(
            """SELECT timestamp, grams_co2_per_kwh, total_generation_mw,
                      clean_percentage
               FROM carbon_intensity
               WHERE timestamp >= ? AND timestamp < ?
               ORDER BY timestamp ASC""",
            (start.isoformat(), end.isoformat()),
        ).fetchall()

        return [dict(row) for row in rows]

    def record_count(self) -> int:
        """Total carbon intensity records stored."""
        row = self._conn.execute(
            "SELECT COUNT(*) as cnt FROM carbon_intensity"
        ).fetchone()
        return row["cnt"] if row else 0

    def date_range(self) -> tuple[str | None, str | None]:
        """Return the earliest and latest timestamps in the store."""
        row = self._conn.execute(
            "SELECT MIN(timestamp) as earliest, MAX(timestamp) as latest FROM carbon_intensity"
        ).fetchone()
        if row:
            return row["earliest"], row["latest"]
        return None, None
