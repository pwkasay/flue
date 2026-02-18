"""Integration tests — weir pipeline with gridcarbon stages.

These tests prove that the two portfolio projects compose correctly:
weir provides the pipeline machinery, gridcarbon provides the
domain-specific stages and data models.
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from weir import Pipeline

from gridcarbon.models.fuel_mix import FuelGeneration, FuelMix
from gridcarbon.sources.emission_factors import NYISOFuelCategory
from gridcarbon.pipeline.ingest import (
    validate,
    make_persist_stage,
    make_event_logging_handler,
    ValidationError,
)

from conftest import requires_postgres

EASTERN = ZoneInfo("America/New_York")


def _make_mix(ts_offset_minutes: int = 0, gas_mw: float = 5000, **overrides) -> FuelMix:
    """Helper to create test FuelMix objects."""
    now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=EASTERN) + timedelta(minutes=ts_offset_minutes)
    fuels = [
        FuelGeneration(fuel=NYISOFuelCategory.NATURAL_GAS, generation_mw=gas_mw),
        FuelGeneration(fuel=NYISOFuelCategory.NUCLEAR, generation_mw=3000),
        FuelGeneration(fuel=NYISOFuelCategory.HYDRO, generation_mw=2000),
        FuelGeneration(fuel=NYISOFuelCategory.WIND, generation_mw=500),
    ]
    return FuelMix(timestamp=now, fuels=fuels, **overrides)


def _make_bad_mix(ts_offset_minutes: int = 0) -> FuelMix:
    """FuelMix with zero generation (should fail validation)."""
    now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=EASTERN) + timedelta(minutes=ts_offset_minutes)
    return FuelMix(
        timestamp=now,
        fuels=[
            FuelGeneration(fuel=NYISOFuelCategory.NATURAL_GAS, generation_mw=0),
            FuelGeneration(fuel=NYISOFuelCategory.NUCLEAR, generation_mw=0),
            FuelGeneration(fuel=NYISOFuelCategory.HYDRO, generation_mw=0),
        ],
    )


# ─── Stage Unit Tests (no pipeline needed) ───


class TestValidateStage:
    """Test the validate stage as a standalone async function."""

    async def test_valid_mix_passes(self):
        mix = _make_mix()
        result = await validate(mix)
        assert result is mix  # Same object, not modified

    async def test_zero_generation_raises(self):
        bad = _make_bad_mix()
        with pytest.raises(ValidationError, match="Zero/negative"):
            await validate(bad)

    async def test_too_few_fuels_raises(self):
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=EASTERN)
        sparse = FuelMix(
            timestamp=now,
            fuels=[
                FuelGeneration(fuel=NYISOFuelCategory.NATURAL_GAS, generation_mw=5000),
                FuelGeneration(fuel=NYISOFuelCategory.NUCLEAR, generation_mw=3000),
            ],
        )
        with pytest.raises(ValidationError, match="Only 2"):
            await validate(sparse)

    async def test_negative_generation_raises(self):
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=EASTERN)
        bad = FuelMix(
            timestamp=now,
            fuels=[
                FuelGeneration(fuel=NYISOFuelCategory.NATURAL_GAS, generation_mw=-100),
                FuelGeneration(fuel=NYISOFuelCategory.NUCLEAR, generation_mw=3000),
                FuelGeneration(fuel=NYISOFuelCategory.HYDRO, generation_mw=2000),
                FuelGeneration(fuel=NYISOFuelCategory.WIND, generation_mw=500),
            ],
        )
        with pytest.raises(ValidationError, match="Negative"):
            await validate(bad)


@requires_postgres
class TestPersistStage:
    """Test the persist stage factory."""

    async def test_persist_saves_to_store(self, async_store):
        persist = make_persist_stage(async_store)
        mix = _make_mix()

        result = await persist(mix)
        assert result is mix
        assert await async_store.record_count() == 1


@requires_postgres
class TestEventLoggingHandler:
    """Test the event logging error handler."""

    async def test_handler_logs_failure(self, async_store):
        from weir import FailedItem

        handler = make_event_logging_handler(async_store)

        failed = FailedItem(
            item=_make_mix(),
            stage_name="validate",
            error=ValidationError("Test error"),
            attempts=1,
        )

        await handler(failed)

        events = await async_store.get_recent_events(limit=10)
        assert len(events) == 1
        assert events[0]["event_type"] == "validate_failure"
        assert "Test error" in events[0]["message"]


# ─── Pipeline Integration Tests ───


@requires_postgres
class TestSeedPipeline:
    """Test the full weir pipeline with gridcarbon stages."""

    async def test_pipeline_processes_valid_data(self, async_store):
        """Valid FuelMix objects flow through validate -> persist."""
        persist = make_persist_stage(async_store)

        mixes = [_make_mix(ts_offset_minutes=i * 5) for i in range(10)]

        async def test_source():
            for m in mixes:
                yield m

        result = await (
            Pipeline("test-seed", channel_capacity=16, drain_timeout=5.0)
            .source(test_source())
            .then(validate)
            .then(persist)
            .on_error(ValidationError)
            .build()
            .run()
        )

        assert result.completed is True
        assert result.dead_letters == 0
        assert await async_store.record_count() == 10

        # Check stage metrics
        assert len(result.stage_metrics) == 2
        validate_metrics = result.stage_metrics[0]
        persist_metrics = result.stage_metrics[1]
        assert validate_metrics["items_in"] == 10
        assert validate_metrics["items_out"] == 10
        assert persist_metrics["items_in"] == 10
        assert persist_metrics["items_out"] == 10

    async def test_pipeline_routes_bad_data_to_dead_letters(self, async_store):
        """Invalid FuelMix records go to dead letters, valid ones pass through."""
        persist = make_persist_stage(async_store)

        items = [
            _make_mix(ts_offset_minutes=0),  # valid
            _make_bad_mix(ts_offset_minutes=5),  # invalid: zero generation
            _make_mix(ts_offset_minutes=10),  # valid
            _make_bad_mix(ts_offset_minutes=15),  # invalid
            _make_mix(ts_offset_minutes=20),  # valid
        ]

        async def test_source():
            for m in items:
                yield m

        pipe = (
            Pipeline("test-errors", channel_capacity=16, drain_timeout=5.0)
            .source(test_source())
            .then(validate)
            .then(persist)
            .on_error(ValidationError)
            .build()
        )

        result = await pipe.run()

        assert result.completed is True
        assert result.dead_letters == 2
        assert await async_store.record_count() == 3

        dead = pipe.dead_letter_items
        assert len(dead) == 2
        assert all("Zero/negative" in str(d.error) for d in dead)

    async def test_pipeline_metrics_include_latency(self, async_store):
        """Pipeline collects per-stage latency percentiles."""
        persist = make_persist_stage(async_store)

        mixes = [_make_mix(ts_offset_minutes=i * 5) for i in range(20)]

        async def test_source():
            for m in mixes:
                yield m

        result = await (
            Pipeline("test-metrics", channel_capacity=32, drain_timeout=5.0)
            .source(test_source())
            .then(validate)
            .then(persist)
            .on_error(ValidationError)
            .build()
            .run()
        )

        for sm in result.stage_metrics:
            assert sm["items_in"] == 20
            assert sm["latency_p50"] is not None
            assert sm["latency_p50"] >= 0
            assert sm["throughput_per_sec"] > 0

    async def test_pipeline_summary_is_readable(self, async_store):
        """PipelineResult.summary() produces human-readable output."""
        persist = make_persist_stage(async_store)

        mixes = [_make_mix(ts_offset_minutes=i * 5) for i in range(5)]

        async def test_source():
            for m in mixes:
                yield m

        result = await (
            Pipeline("test-summary", channel_capacity=16, drain_timeout=5.0)
            .source(test_source())
            .then(validate)
            .then(persist)
            .on_error(ValidationError)
            .build()
            .run()
        )

        summary = result.summary()
        assert "test-summary" in summary
        assert "completed" in summary
        assert "validate" in summary
        assert "persist" in summary

    async def test_topology_shows_stage_wiring(self, async_store):
        """Pipeline.topology shows the stage graph for debugging."""
        persist = make_persist_stage(async_store)

        async def empty_source():
            return
            yield  # Make it an async generator

        pipe = (
            Pipeline("test-topo", channel_capacity=16)
            .source(empty_source())
            .then(validate)
            .then(persist)
            .on_error(ValidationError)
            .build()
        )

        topo = pipe.topology
        assert "test-topo" in topo
        assert "validate" in topo
        assert "persist" in topo

    async def test_pipeline_with_event_logging(self, async_store):
        """Event logging handler records failures to ingestion_events."""
        persist = make_persist_stage(async_store)
        handler = make_event_logging_handler(async_store)

        items = [
            _make_mix(ts_offset_minutes=0),
            _make_bad_mix(ts_offset_minutes=5),
            _make_mix(ts_offset_minutes=10),
        ]

        async def test_source():
            for m in items:
                yield m

        result = await (
            Pipeline("test-events", channel_capacity=16, drain_timeout=5.0)
            .source(test_source())
            .then(validate)
            .then(persist)
            .on_error(ValidationError, handler)
            .build()
            .run()
        )

        assert result.completed is True
        assert await async_store.record_count() == 2

        # Check that the failure was logged
        events = await async_store.get_recent_events(limit=10, event_type="validate_failure")
        assert len(events) == 1
        assert "Zero/negative" in events[0]["message"]
