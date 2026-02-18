"""Integration tests — asyncpipe pipeline with gridcarbon stages.

These tests prove that the two portfolio projects compose correctly:
asyncpipe provides the pipeline machinery, gridcarbon provides the
domain-specific stages and data models.
"""


import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from asyncpipe import Pipeline, stage, PipelineResult

from gridcarbon.models.fuel_mix import CarbonIntensity, FuelGeneration, FuelMix
from gridcarbon.models.exceptions import StoreError
from gridcarbon.sources.emission_factors import NYISOFuelCategory
from gridcarbon.storage.store import Store
from gridcarbon.pipeline.ingest import validate, make_persist_stage, ValidationError

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

    @pytest.mark.asyncio
    async def test_valid_mix_passes(self):
        mix = _make_mix()
        result = await validate(mix)
        assert result is mix  # Same object, not modified

    @pytest.mark.asyncio
    async def test_zero_generation_raises(self):
        bad = _make_bad_mix()
        with pytest.raises(ValidationError, match="Zero/negative"):
            await validate(bad)

    @pytest.mark.asyncio
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

    @pytest.mark.asyncio
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


class TestPersistStage:
    """Test the persist stage factory."""

    @pytest.mark.asyncio
    async def test_persist_saves_to_store(self):
        store = Store(db_path=Path(tempfile.mktemp(suffix=".db")))
        persist = make_persist_stage(store)
        mix = _make_mix()

        result = await persist(mix)
        assert result is mix
        assert store.record_count() == 1
        store.close()


# ─── Pipeline Integration Tests ───


class TestSeedPipeline:
    """Test the full asyncpipe pipeline with gridcarbon stages."""

    @pytest.mark.asyncio
    async def test_pipeline_processes_valid_data(self):
        """Valid FuelMix objects flow through validate → persist."""
        store = Store(db_path=Path(tempfile.mktemp(suffix=".db")))
        persist = make_persist_stage(store)

        # Create test data
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
        assert store.record_count() == 10

        # Check stage metrics
        assert len(result.stage_metrics) == 2
        validate_metrics = result.stage_metrics[0]
        persist_metrics = result.stage_metrics[1]
        assert validate_metrics["items_in"] == 10
        assert validate_metrics["items_out"] == 10
        assert persist_metrics["items_in"] == 10
        assert persist_metrics["items_out"] == 10

        store.close()

    @pytest.mark.asyncio
    async def test_pipeline_routes_bad_data_to_dead_letters(self):
        """Invalid FuelMix records go to dead letters, valid ones pass through."""
        store = Store(db_path=Path(tempfile.mktemp(suffix=".db")))
        persist = make_persist_stage(store)

        # Mix of valid and invalid data
        items = [
            _make_mix(ts_offset_minutes=0),     # valid
            _make_bad_mix(ts_offset_minutes=5),  # invalid: zero generation
            _make_mix(ts_offset_minutes=10),    # valid
            _make_bad_mix(ts_offset_minutes=15), # invalid
            _make_mix(ts_offset_minutes=20),    # valid
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
        assert result.dead_letters == 2  # Two bad mixes
        assert store.record_count() == 3  # Three valid mixes persisted

        # Dead letters are inspectable
        dead = pipe.dead_letter_items
        assert len(dead) == 2
        assert all("Zero/negative" in str(d.error) for d in dead)

        store.close()

    @pytest.mark.asyncio
    async def test_pipeline_metrics_include_latency(self):
        """Pipeline collects per-stage latency percentiles."""
        store = Store(db_path=Path(tempfile.mktemp(suffix=".db")))
        persist = make_persist_stage(store)

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

        # Metrics should include latency stats
        for sm in result.stage_metrics:
            assert sm["items_in"] == 20
            assert sm["latency_p50"] is not None
            assert sm["latency_p50"] >= 0
            assert sm["throughput_per_sec"] > 0

        store.close()

    @pytest.mark.asyncio
    async def test_pipeline_summary_is_readable(self):
        """PipelineResult.summary() produces human-readable output."""
        store = Store(db_path=Path(tempfile.mktemp(suffix=".db")))
        persist = make_persist_stage(store)

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

        store.close()

    @pytest.mark.asyncio
    async def test_topology_shows_stage_wiring(self):
        """Pipeline.topology shows the stage graph for debugging."""
        store = Store(db_path=Path(tempfile.mktemp(suffix=".db")))
        persist = make_persist_stage(store)

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

        store.close()
