"""Tests for gridcarbon.

Tests cover:
1. Domain models (FuelMix, CarbonIntensity, Forecast)
2. Emission factor lookups
3. NYISO CSV parsing
4. Heuristic forecaster
5. Storage read/write (Postgres)
6. Forecast window finding
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from gridcarbon.models.fuel_mix import (
    CarbonIntensity,
    FuelGeneration,
    FuelMix,
)
from gridcarbon.models.forecast import Forecast, HourlyForecast
from gridcarbon.models.exceptions import UnknownFuelCategory
from gridcarbon.sources.emission_factors import (
    NYISOFuelCategory,
    get_factor,
    all_factors_summary,
)
from gridcarbon.forecaster.heuristic import HeuristicForecaster
from gridcarbon.sources.weather import WeatherSnapshot

from conftest import requires_postgres

EASTERN = ZoneInfo("America/New_York")


# ── Emission Factor Tests ──


class TestEmissionFactors:
    def test_gas_has_positive_factor(self):
        assert get_factor(NYISOFuelCategory.NATURAL_GAS) > 0

    def test_nuclear_is_zero(self):
        assert get_factor(NYISOFuelCategory.NUCLEAR) == 0

    def test_wind_is_zero(self):
        assert get_factor(NYISOFuelCategory.WIND) == 0

    def test_all_categories_have_factors(self):
        for cat in NYISOFuelCategory:
            factor = get_factor(cat)
            assert isinstance(factor, (int, float))

    def test_fuel_label_parsing(self):
        assert NYISOFuelCategory.from_nyiso_label("Natural Gas") == NYISOFuelCategory.NATURAL_GAS
        assert NYISOFuelCategory.from_nyiso_label("  Wind  ") == NYISOFuelCategory.WIND
        assert NYISOFuelCategory.from_nyiso_label("Dual Fuel") == NYISOFuelCategory.DUAL_FUEL

    def test_unknown_fuel_raises(self):
        with pytest.raises(UnknownFuelCategory):
            NYISOFuelCategory.from_nyiso_label("Unobtanium")

    def test_factors_summary_is_serializable(self):
        summary = all_factors_summary()
        assert len(summary) == 7
        assert all("fuel" in f and "grams_co2_per_kwh" in f for f in summary)


# ── CarbonIntensity Tests ──


class TestCarbonIntensity:
    def test_unit_conversions(self):
        ci = CarbonIntensity(grams_co2_per_kwh=300)
        assert ci.kg_co2_per_kwh == pytest.approx(0.3)
        assert ci.kg_co2_per_mwh == pytest.approx(300)

    def test_category_classification(self):
        assert CarbonIntensity(grams_co2_per_kwh=100).category == "very_clean"
        assert CarbonIntensity(grams_co2_per_kwh=200).category == "clean"
        assert CarbonIntensity(grams_co2_per_kwh=300).category == "moderate"
        assert CarbonIntensity(grams_co2_per_kwh=400).category == "dirty"
        assert CarbonIntensity(grams_co2_per_kwh=500).category == "very_dirty"

    def test_recommendation_exists_for_all_categories(self):
        for g in [100, 200, 300, 400, 500]:
            ci = CarbonIntensity(grams_co2_per_kwh=g)
            assert len(ci.recommendation) > 0
            assert len(ci.category_label) > 0

    def test_comparison_operators(self):
        low = CarbonIntensity(grams_co2_per_kwh=100)
        high = CarbonIntensity(grams_co2_per_kwh=400)
        assert low < high
        assert high > low
        assert low <= low

    def test_arithmetic(self):
        a = CarbonIntensity(grams_co2_per_kwh=200)
        b = CarbonIntensity(grams_co2_per_kwh=400)
        avg = (a + b) / 2
        assert avg.grams_co2_per_kwh == pytest.approx(300)


# ── FuelMix Tests ──


class TestFuelMix:
    def _make_mix(self, gas_mw=5000, nuclear_mw=3000, hydro_mw=2000, wind_mw=500):
        now = datetime.now(EASTERN)
        return FuelMix(
            timestamp=now,
            fuels=[
                FuelGeneration(fuel=NYISOFuelCategory.NATURAL_GAS, generation_mw=gas_mw),
                FuelGeneration(fuel=NYISOFuelCategory.NUCLEAR, generation_mw=nuclear_mw),
                FuelGeneration(fuel=NYISOFuelCategory.HYDRO, generation_mw=hydro_mw),
                FuelGeneration(fuel=NYISOFuelCategory.WIND, generation_mw=wind_mw),
            ],
        )

    def test_carbon_intensity_calculated_at_init(self):
        mix = self._make_mix()
        ci = mix.carbon_intensity
        assert ci.grams_co2_per_kwh > 0

    def test_all_clean_is_zero(self):
        mix = self._make_mix(gas_mw=0)
        ci = mix.carbon_intensity
        assert ci.grams_co2_per_kwh == 0

    def test_total_generation(self):
        mix = self._make_mix(gas_mw=5000, nuclear_mw=3000, hydro_mw=2000, wind_mw=500)
        assert mix.total_generation_mw == 10500

    def test_clean_percentage(self):
        mix = self._make_mix(gas_mw=5000, nuclear_mw=3000, hydro_mw=2000, wind_mw=500)
        # Clean: nuclear + hydro + wind = 5500 / 10500 = 52.38%
        assert mix.clean_percentage == pytest.approx(52.38, abs=0.1)

    def test_fuel_breakdown_sorted_descending(self):
        mix = self._make_mix()
        breakdown = mix.fuel_breakdown
        values = list(breakdown.values())
        assert values == sorted(values, reverse=True)

    def test_to_dict_is_complete(self):
        mix = self._make_mix()
        d = mix.to_dict()
        assert "timestamp" in d
        assert "carbon_intensity_gco2_kwh" in d
        assert "total_generation_mw" in d
        assert "clean_percentage" in d
        assert "fuel_breakdown_mw" in d


# ── Forecast Tests ──


class TestForecast:
    def _make_forecast(self, hours=24):
        now = datetime.now(EASTERN)
        hourly = []
        for h in range(hours):
            # Simulate daily pattern: cleanest at 3am, dirtiest at 6pm
            hour_of_day = (now + timedelta(hours=h)).hour
            ci_value = 200 + 150 * abs(hour_of_day - 3) / 15  # Rises from 3am
            hourly.append(
                HourlyForecast(
                    hour=now + timedelta(hours=h),
                    predicted_intensity=CarbonIntensity(grams_co2_per_kwh=ci_value),
                    confidence="high" if h < 6 else "medium",
                )
            )
        return Forecast(generated_at=now, hourly=hourly)

    def test_cleanest_window(self):
        fc = self._make_forecast()
        cleanest = fc.cleanest_window(3)
        assert cleanest is not None
        assert cleanest.label == "cleanest"
        assert cleanest.duration_hours == 3

    def test_dirtiest_window(self):
        fc = self._make_forecast()
        dirtiest = fc.dirtiest_window(3)
        assert dirtiest is not None
        assert dirtiest.label == "dirtiest"

    def test_cleanest_is_less_than_dirtiest(self):
        fc = self._make_forecast()
        cleanest = fc.cleanest_window(3)
        dirtiest = fc.dirtiest_window(3)
        assert cleanest.average_intensity < dirtiest.average_intensity

    def test_summary_is_nonempty(self):
        fc = self._make_forecast()
        assert len(fc.summary) > 50

    def test_to_dict(self):
        fc = self._make_forecast()
        d = fc.to_dict()
        assert d["forecast_hours"] == 24
        assert len(d["hourly"]) == 24
        assert d["cleanest_3h_window"] is not None


# ── Storage Tests (Postgres) ──


@requires_postgres
class TestStore:
    def test_save_and_retrieve(self, sync_store):
        now = datetime.now(EASTERN)
        mix = FuelMix(
            timestamp=now,
            fuels=[
                FuelGeneration(fuel=NYISOFuelCategory.NATURAL_GAS, generation_mw=5000),
                FuelGeneration(fuel=NYISOFuelCategory.NUCLEAR, generation_mw=3000),
            ],
        )

        sync_store.save_fuel_mix(mix)
        assert sync_store.record_count() == 1

        latest = sync_store.get_latest_intensity()
        assert latest is not None
        assert latest["grams_co2_per_kwh"] > 0

    def test_bulk_save(self, sync_store):
        now = datetime.now(EASTERN)
        mixes = []
        for i in range(10):
            mixes.append(
                FuelMix(
                    timestamp=now + timedelta(minutes=5 * i),
                    fuels=[
                        FuelGeneration(fuel=NYISOFuelCategory.NATURAL_GAS, generation_mw=5000),
                        FuelGeneration(fuel=NYISOFuelCategory.NUCLEAR, generation_mw=3000),
                    ],
                )
            )

        count = sync_store.save_fuel_mixes(mixes)
        assert count == 10
        assert sync_store.record_count() == 10

    def test_hourly_averages(self, sync_store):
        # Use timezone-aware timestamps for Postgres
        base = datetime(2024, 6, 15, 0, 0, 0, tzinfo=EASTERN)

        for h in range(24):
            ts = base.replace(hour=h)
            mix = FuelMix(
                timestamp=ts,
                fuels=[
                    FuelGeneration(
                        fuel=NYISOFuelCategory.NATURAL_GAS,
                        generation_mw=3000 + h * 100,
                    ),
                    FuelGeneration(fuel=NYISOFuelCategory.NUCLEAR, generation_mw=3000),
                ],
            )
            sync_store.save_fuel_mix(mix)

        avgs = sync_store.get_hourly_averages()
        assert len(avgs) >= 20
        # Later hours have more gas, so higher CI
        if 3 in avgs and 20 in avgs:
            assert avgs[20] > avgs[3]

    def test_date_range(self, sync_store):
        now = datetime.now(EASTERN)

        for i in range(5):
            mix = FuelMix(
                timestamp=now + timedelta(days=i),
                fuels=[
                    FuelGeneration(fuel=NYISOFuelCategory.NATURAL_GAS, generation_mw=5000),
                ],
            )
            sync_store.save_fuel_mix(mix)

        earliest, latest = sync_store.date_range()
        assert earliest is not None
        assert latest is not None

    def test_log_event(self, sync_store):
        sync_store.log_event(
            event_type="test_event",
            stage_name="test_stage",
            message="Test message",
            details={"key": "value"},
        )

        events = sync_store.get_recent_events(limit=10)
        assert len(events) == 1
        assert events[0]["event_type"] == "test_event"
        assert events[0]["stage_name"] == "test_stage"
        assert events[0]["message"] == "Test message"

    def test_get_recent_events_filter(self, sync_store):
        sync_store.log_event(event_type="success", message="ok")
        sync_store.log_event(event_type="failure", message="bad")
        sync_store.log_event(event_type="failure", message="worse")

        all_events = sync_store.get_recent_events(limit=10)
        assert len(all_events) == 3

        failures = sync_store.get_recent_events(limit=10, event_type="failure")
        assert len(failures) == 2

    def test_ingestion_status(self, sync_store):
        status = sync_store.get_ingestion_status()
        assert status["total_records"] == 0
        assert status["is_active"] is False

        # Add a record
        now = datetime.now(EASTERN)
        mix = FuelMix(
            timestamp=now,
            fuels=[
                FuelGeneration(fuel=NYISOFuelCategory.NATURAL_GAS, generation_mw=5000),
            ],
        )
        sync_store.save_fuel_mix(mix)

        status = sync_store.get_ingestion_status()
        assert status["total_records"] == 1


# ── Forecaster Tests ──


@requires_postgres
class TestHeuristicForecaster:
    def test_forecast_without_data(self, sync_store):
        """Should work with fallback profile even with empty store."""
        forecaster = HeuristicForecaster(sync_store)
        fc = forecaster.forecast(hours=24)
        assert fc.forecast_hours == 24
        assert all(h.predicted_intensity.grams_co2_per_kwh > 0 for h in fc.hourly)

    def test_forecast_with_weather(self, sync_store):
        """Weather corrections should shift the forecast."""
        forecaster = HeuristicForecaster(sync_store)
        now = datetime.now(EASTERN)

        hot_weather = [
            WeatherSnapshot(
                timestamp=now + timedelta(hours=h),
                temperature_f=100,
                wind_speed_80m_mph=5,
                cloud_cover_pct=0,
            )
            for h in range(24)
        ]

        mild_weather = [
            WeatherSnapshot(
                timestamp=now + timedelta(hours=h),
                temperature_f=70,
                wind_speed_80m_mph=20,
                cloud_cover_pct=0,
            )
            for h in range(24)
        ]

        hot_fc = forecaster.forecast(hours=24, weather=hot_weather)
        mild_fc = forecaster.forecast(hours=24, weather=mild_weather)

        hot_avg = sum(h.predicted_intensity.grams_co2_per_kwh for h in hot_fc.hourly) / 24
        mild_avg = sum(h.predicted_intensity.grams_co2_per_kwh for h in mild_fc.hourly) / 24

        assert hot_avg > mild_avg

    def test_persistence_blend(self, sync_store):
        """Near-term forecast should be influenced by current actual CI."""
        forecaster = HeuristicForecaster(sync_store)

        high_current = CarbonIntensity(grams_co2_per_kwh=500)
        fc = forecaster.forecast(hours=24, current_intensity=high_current)

        first_hour = fc.hourly[0].predicted_intensity.grams_co2_per_kwh
        last_hour = fc.hourly[-1].predicted_intensity.grams_co2_per_kwh
        assert first_hour > last_hour

    def test_forecast_max_48_hours(self, sync_store):
        forecaster = HeuristicForecaster(sync_store)
        fc = forecaster.forecast(hours=100)
        assert fc.forecast_hours == 48


# ── NYISO CSV Parsing Tests ──


class TestNYISOParsing:
    def test_parse_csv(self):
        """Test parsing of NYISO fuel mix CSV format."""
        from gridcarbon.sources.nyiso import _parse_csv
        from datetime import date

        csv_text = """Time Stamp,Time Zone,Fuel Category,Gen MW
01/15/2024 00:05:00,EST,Dual Fuel,4521
01/15/2024 00:05:00,EST,Natural Gas,3200
01/15/2024 00:05:00,EST,Nuclear,3100
01/15/2024 00:05:00,EST,Other Fossil Fuels,50
01/15/2024 00:05:00,EST,Other Renewables,200
01/15/2024 00:05:00,EST,Wind,1500
01/15/2024 00:05:00,EST,Hydro,2800
01/15/2024 00:10:00,EST,Dual Fuel,4500
01/15/2024 00:10:00,EST,Natural Gas,3150
01/15/2024 00:10:00,EST,Nuclear,3100
01/15/2024 00:10:00,EST,Other Fossil Fuels,48
01/15/2024 00:10:00,EST,Other Renewables,195
01/15/2024 00:10:00,EST,Wind,1520
01/15/2024 00:10:00,EST,Hydro,2810"""

        mixes = _parse_csv(csv_text, date(2024, 1, 15))
        assert len(mixes) == 2  # Two 5-minute intervals

        mix = mixes[0]
        assert len(mix.fuels) == 7
        assert mix.total_generation_mw > 0
        assert mix.carbon_intensity.grams_co2_per_kwh > 0
        assert 0 < mix.clean_percentage < 100

    def test_parse_csv_handles_empty(self):
        from gridcarbon.sources.nyiso import _parse_csv
        from datetime import date

        mixes = _parse_csv("", date(2024, 1, 15))
        assert len(mixes) == 0

    def test_parse_csv_handles_bad_rows(self):
        from gridcarbon.sources.nyiso import _parse_csv
        from datetime import date

        csv_text = """Time Stamp,Time Zone,Fuel Category,Gen MW
01/15/2024 00:05:00,EST,Natural Gas,3200
01/15/2024 00:05:00,EST,Unknown Fuel,999
01/15/2024 00:05:00,EST,Nuclear,3100"""

        # Should parse what it can, skip unknown fuel
        mixes = _parse_csv(csv_text, date(2024, 1, 15))
        assert len(mixes) == 1
        assert len(mixes[0].fuels) == 2  # Gas + Nuclear, skipped unknown
