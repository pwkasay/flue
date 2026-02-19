"""Async PostgreSQL storage using asyncpg.

Used by the weir pipeline and FastAPI endpoints where async context
is natural. Mirrors the sync Store API.
"""

import json
import logging
import os
from datetime import date, datetime, timezone
from typing import Any

import asyncpg

from ..models.fuel_mix import FuelMix
from ..models.exceptions import StoreError

logger = logging.getLogger("gridcarbon.storage")

DEFAULT_DSN = "postgresql://gridcarbon:gridcarbon@localhost:5432/gridcarbon"


class AsyncStore:
    """Async PostgreSQL store backed by an asyncpg connection pool.

    Usage:
        store = await AsyncStore.create()
        await store.save_fuel_mix(fuel_mix)
        await store.close()
    """

    def __init__(self, pool: asyncpg.Pool, dsn: str) -> None:
        self._pool = pool
        self.dsn = dsn

    @classmethod
    async def create(cls, dsn: str | None = None) -> "AsyncStore":
        dsn = dsn or os.environ.get("DATABASE_URL", DEFAULT_DSN)
        pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
        return cls(pool, dsn)

    async def close(self) -> None:
        await self._pool.close()

    # ── Write ──

    async def save_fuel_mix(self, mix: FuelMix) -> None:
        """Save a FuelMix snapshot (fuel breakdown + computed carbon intensity)."""
        ts = mix.timestamp
        ci = mix.carbon_intensity

        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    for fuel in mix.fuels:
                        await conn.execute(
                            """INSERT INTO fuel_mix (timestamp, fuel_category, generation_mw)
                               VALUES ($1, $2, $3)
                               ON CONFLICT (timestamp, fuel_category)
                               DO UPDATE SET generation_mw = EXCLUDED.generation_mw""",
                            ts,
                            fuel.fuel.value,
                            fuel.generation_mw,
                        )

                    await conn.execute(
                        """INSERT INTO carbon_intensity
                           (timestamp, grams_co2_per_kwh, total_generation_mw,
                            clean_percentage, fuel_breakdown_json)
                           VALUES ($1, $2, $3, $4, $5)
                           ON CONFLICT (timestamp)
                           DO UPDATE SET grams_co2_per_kwh = EXCLUDED.grams_co2_per_kwh,
                                         total_generation_mw = EXCLUDED.total_generation_mw,
                                         clean_percentage = EXCLUDED.clean_percentage,
                                         fuel_breakdown_json = EXCLUDED.fuel_breakdown_json""",
                        ts,
                        ci.grams_co2_per_kwh,
                        mix.total_generation_mw,
                        mix.clean_percentage,
                        json.dumps(mix.fuel_breakdown),
                    )
        except asyncpg.PostgresError as e:
            raise StoreError(f"Failed to save fuel mix: {e}") from e

    async def save_fuel_mixes(self, mixes: list[FuelMix]) -> int:
        """Bulk save fuel mix snapshots. Returns count saved."""
        count = 0
        for mix in mixes:
            try:
                await self.save_fuel_mix(mix)
                count += 1
            except StoreError as e:
                logger.warning("Skipping fuel mix save: %s", e)
        return count

    async def save_weather(
        self, timestamp: datetime, temp_f: float, wind_mph: float, cloud_pct: float
    ) -> None:
        """Save a weather observation."""
        try:
            await self._pool.execute(
                """INSERT INTO weather
                   (timestamp, temperature_f, wind_speed_80m_mph, cloud_cover_pct)
                   VALUES ($1, $2, $3, $4)
                   ON CONFLICT (timestamp)
                   DO UPDATE SET temperature_f = EXCLUDED.temperature_f,
                                 wind_speed_80m_mph = EXCLUDED.wind_speed_80m_mph,
                                 cloud_cover_pct = EXCLUDED.cloud_cover_pct""",
                timestamp,
                temp_f,
                wind_mph,
                cloud_pct,
            )
        except asyncpg.PostgresError as e:
            raise StoreError(f"Failed to save weather: {e}") from e

    # ── Read ──

    async def get_carbon_intensity(self, hours: int = 24) -> list[dict[str, Any]]:
        """Get recent carbon intensity data points."""
        rows = await self._pool.fetch(
            """SELECT timestamp, grams_co2_per_kwh, total_generation_mw,
                      clean_percentage, fuel_breakdown_json
               FROM carbon_intensity
               WHERE timestamp > NOW() - make_interval(hours => $1)
               ORDER BY timestamp ASC""",
            hours,
        )

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

    async def get_latest_intensity(self) -> dict[str, Any] | None:
        """Get the most recent carbon intensity record."""
        row = await self._pool.fetchrow(
            """SELECT timestamp, grams_co2_per_kwh, total_generation_mw,
                      clean_percentage, fuel_breakdown_json
               FROM carbon_intensity
               ORDER BY timestamp DESC LIMIT 1""",
        )

        if row is None:
            return None

        return {
            "timestamp": row["timestamp"].isoformat(),
            "grams_co2_per_kwh": row["grams_co2_per_kwh"],
            "total_generation_mw": row["total_generation_mw"],
            "clean_percentage": row["clean_percentage"],
            "fuel_breakdown": row["fuel_breakdown_json"] or {},
        }

    async def get_hourly_averages(
        self, month: int | None = None, day_of_week: int | None = None
    ) -> dict[int, float]:
        """Get average carbon intensity by hour of day."""
        conditions = []
        params: list[Any] = []
        param_idx = 1

        if month is not None:
            conditions.append(f"EXTRACT(MONTH FROM timestamp) = ${param_idx}")
            params.append(month)
            param_idx += 1
        if day_of_week is not None:
            pg_dow = (day_of_week + 1) % 7
            conditions.append(f"EXTRACT(DOW FROM timestamp) = ${param_idx}")
            params.append(pg_dow)
            param_idx += 1

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        rows = await self._pool.fetch(
            f"""SELECT EXTRACT(HOUR FROM timestamp)::int AS hour,
                       AVG(grams_co2_per_kwh) AS avg_ci
                FROM carbon_intensity
                {where}
                GROUP BY hour
                ORDER BY hour""",
            *params,
        )

        return {row["hour"]: row["avg_ci"] for row in rows}

    async def get_intensity_range(self, start: date, end: date) -> list[dict[str, Any]]:
        """Get carbon intensity data for a date range."""
        rows = await self._pool.fetch(
            """SELECT timestamp, grams_co2_per_kwh, total_generation_mw,
                      clean_percentage
               FROM carbon_intensity
               WHERE timestamp >= $1 AND timestamp < $2
               ORDER BY timestamp ASC""",
            start,
            end,
        )

        return [
            {
                "timestamp": row["timestamp"].isoformat(),
                "grams_co2_per_kwh": row["grams_co2_per_kwh"],
                "total_generation_mw": row["total_generation_mw"],
                "clean_percentage": row["clean_percentage"],
            }
            for row in rows
        ]

    async def record_count(self) -> int:
        """Total carbon intensity records stored."""
        row = await self._pool.fetchrow("SELECT COUNT(*) AS cnt FROM carbon_intensity")
        return row["cnt"] if row else 0

    async def date_range(self) -> tuple[str | None, str | None]:
        """Return the earliest and latest timestamps in the store."""
        row = await self._pool.fetchrow(
            "SELECT MIN(timestamp) AS earliest, MAX(timestamp) AS latest FROM carbon_intensity"
        )
        if row and row["earliest"]:
            return row["earliest"].isoformat(), row["latest"].isoformat()
        return None, None

    # ── Ingestion Events ──

    async def log_event(
        self,
        event_type: str,
        stage_name: str | None = None,
        message: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Record an ingestion event for admin visibility."""
        try:
            await self._pool.execute(
                """INSERT INTO ingestion_events
                   (event_type, stage_name, message, details_json)
                   VALUES ($1, $2, $3, $4)""",
                event_type,
                stage_name,
                message,
                json.dumps(details) if details else None,
            )
        except asyncpg.PostgresError as e:
            logger.warning("Failed to log event: %s", e)

    async def get_recent_events(
        self, limit: int = 50, event_type: str | None = None
    ) -> list[dict[str, Any]]:
        """Query recent ingestion events."""
        if event_type:
            rows = await self._pool.fetch(
                """SELECT timestamp, event_type, stage_name, message, details_json
                   FROM ingestion_events
                   WHERE event_type = $1
                   ORDER BY timestamp DESC LIMIT $2""",
                event_type,
                limit,
            )
        else:
            rows = await self._pool.fetch(
                """SELECT timestamp, event_type, stage_name, message, details_json
                   FROM ingestion_events
                   ORDER BY timestamp DESC LIMIT $1""",
                limit,
            )

        return [
            {
                "timestamp": row["timestamp"].isoformat(),
                "event_type": row["event_type"],
                "stage_name": row["stage_name"],
                "message": row["message"],
                "details": row["details_json"] if row["details_json"] else None,
            }
            for row in rows
        ]

    async def save_pipeline_metrics(
        self, pipeline_name: str, snapshots: list[dict[str, Any]]
    ) -> None:
        """Bulk-insert StageMetricsSnapshot dicts into pipeline_metrics."""
        try:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    for s in snapshots:
                        await conn.execute(
                            """INSERT INTO pipeline_metrics
                               (pipeline_name, stage_name, items_in, items_out,
                                items_errored, items_retried, error_rate,
                                throughput_per_sec, latency_p50, latency_p95,
                                latency_p99, queue_depth, queue_utilization)
                               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)""",
                            pipeline_name,
                            s["stage"],
                            s["items_in"],
                            s["items_out"],
                            s["items_errored"],
                            s.get("items_retried", 0),
                            s.get("error_rate"),
                            s.get("throughput_per_sec"),
                            s.get("latency_p50"),
                            s.get("latency_p95"),
                            s.get("latency_p99"),
                            s.get("queue_depth"),
                            s.get("queue_utilization"),
                        )
        except asyncpg.PostgresError as e:
            logger.warning("Failed to save pipeline metrics: %s", e)

    async def get_pipeline_metrics(
        self, pipeline_name: str, hours: int = 1
    ) -> list[dict[str, Any]]:
        """Get recent pipeline metrics for a named pipeline."""
        rows = await self._pool.fetch(
            """SELECT timestamp, stage_name, items_in, items_out,
                      items_errored, items_retried, error_rate,
                      throughput_per_sec, latency_p50, latency_p95,
                      latency_p99, queue_depth, queue_utilization
               FROM pipeline_metrics
               WHERE pipeline_name = $1
                 AND timestamp > NOW() - make_interval(hours => $2)
               ORDER BY timestamp DESC""",
            pipeline_name,
            hours,
        )
        return [
            {
                "timestamp": row["timestamp"].isoformat(),
                "stage_name": row["stage_name"],
                "items_in": row["items_in"],
                "items_out": row["items_out"],
                "items_errored": row["items_errored"],
                "items_retried": row["items_retried"],
                "error_rate": row["error_rate"],
                "throughput_per_sec": row["throughput_per_sec"],
                "latency_p50": row["latency_p50"],
                "latency_p95": row["latency_p95"],
                "latency_p99": row["latency_p99"],
                "queue_depth": row["queue_depth"],
                "queue_utilization": row["queue_utilization"],
            }
            for row in rows
        ]

    async def get_weather_freshness(self) -> dict[str, Any]:
        """Query weather table freshness for admin status."""
        row = await self._pool.fetchrow(
            """SELECT
                 MAX(timestamp) AS latest,
                 (SELECT COUNT(*) FROM weather
                  WHERE timestamp > NOW() - INTERVAL '1 hour') AS records_last_hour
               FROM weather"""
        )

        latest = row["latest"] if row else None
        records_last_hour = row["records_last_hour"] if row else 0

        if latest:
            from datetime import timezone

            age = datetime.now(timezone.utc) - latest.astimezone(timezone.utc)
            if age.total_seconds() < 7200:  # 2 hours (weather updates hourly)
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

    async def get_ingestion_status(self) -> dict[str, Any]:
        """Derived ingestion status for the admin dashboard."""
        count = await self.record_count()
        earliest, latest = await self.date_range()

        stats = await self._pool.fetchrow(
            """SELECT
                 (SELECT COUNT(*) FROM carbon_intensity
                  WHERE timestamp > NOW() - INTERVAL '1 hour') AS records_last_hour,
                 (SELECT COUNT(*) FROM ingestion_events
                  WHERE event_type IN ('validation_failure', 'persist_failure')
                  AND timestamp > NOW() - INTERVAL '1 hour') AS errors_last_hour"""
        )

        is_active = False
        if latest:
            latest_dt = datetime.fromisoformat(latest)
            age = datetime.now(timezone.utc) - latest_dt.astimezone(timezone.utc)
            is_active = age.total_seconds() < 600

        return {
            "is_active": is_active,
            "last_data_at": latest,
            "records_last_hour": stats["records_last_hour"] if stats else 0,
            "errors_last_hour": stats["errors_last_hour"] if stats else 0,
            "total_records": count,
            "earliest": earliest,
            "latest": latest,
        }
