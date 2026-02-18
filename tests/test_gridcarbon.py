"""Tests for gridcarbon.

Tests cover:
1. Domain models (FuelMix, CarbonIntensity, Forecast)
2. Emission factor lookups
3. NYISO CSV parsing
4. Heuristic forecaster
5. Storage read/write
6. Forecast window finding
"""


import tempfile
from datetime import datetime, timedelta
from pathlib import Path
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
from gridcarbon.storage.store import Store
from gridcarbon.forecaster.heuristic import HeuristicForecaster
from gridcarbon.sources.weather import WeatherSnapshot

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


# ── Storage Tests ──


class TestStore:
    def _temp_store(self):
        return Store(db_path=Path(tempfile.mktemp(suffix=".db")))

    def test_save_and_retrieve(self):
        store = self._temp_store()
        now = datetime.now(EASTERN)
        mix = FuelMix(
            timestamp=now,
            fuels=[
                FuelGeneration(fuel=NYISOFuelCategory.NATURAL_GAS, generation_mw=5000),
                FuelGeneration(fuel=NYISOFuelCategory.NUCLEAR, generation_mw=3000),
            ],
        )

        store.save_fuel_mix(mix)
        assert store.record_count() == 1

        latest = store.get_latest_intensity()
        assert latest is not None
        assert latest["grams_co2_per_kwh"] > 0
        store.close()

    def test_bulk_save(self):
        store = self._temp_store()
        now = datetime.now(EASTERN)
        mixes = []
        for i in range(10):
            mixes.append(FuelMix(
                timestamp=now + timedelta(minutes=5 * i),
                fuels=[
                    FuelGeneration(fuel=NYISOFuelCategory.NATURAL_GAS, generation_mw=5000),
                    FuelGeneration(fuel=NYISOFuelCategory.NUCLEAR, generation_mw=3000),
                ],
            ))

        count = store.save_fuel_mixes(mixes)
        assert count == 10
        assert store.record_count() == 10
        store.close()

    def test_hourly_averages(self):
        store = self._temp_store()
        # Use naive UTC timestamps so SQLite strftime works correctly
        base = datetime(2024, 6, 15, 0, 0, 0)

        # Insert data for multiple hours
        for h in range(24):
            ts = base.replace(hour=h)
            mix = FuelMix(
                timestamp=ts,
                fuels=[
                    FuelGeneration(
                        fuel=NYISOFuelCategory.NATURAL_GAS,
                        generation_mw=3000 + h * 100,  # More gas at later hours
                    ),
                    FuelGeneration(fuel=NYISOFuelCategory.NUCLEAR, generation_mw=3000),
                ],
            )
            store.save_fuel_mix(mix)

        avgs = store.get_hourly_averages()
        assert len(avgs) >= 20  # Should have most hours covered
        # CI = gas_mw * 450 / (gas_mw + 3000)
        # h=3: 3300*450/6300 = 235.7
        # h=20: 5000*450/8000 = 281.25
        # So later hours should have higher CI
        if 3 in avgs and 20 in avgs:
            assert avgs[20] > avgs[3]
        store.close()

    def test_date_range(self):
        store = self._temp_store()
        now = datetime.now(EASTERN)

        for i in range(5):
            mix = FuelMix(
                timestamp=now + timedelta(days=i),
                fuels=[
                    FuelGeneration(fuel=NYISOFuelCategory.NATURAL_GAS, generation_mw=5000),
                ],
            )
            store.save_fuel_mix(mix)

        earliest, latest = store.date_range()
        assert earliest is not None
        assert latest is not None
        store.close()


# ── Forecaster Tests ──


class TestHeuristicForecaster:
    def _make_forecaster(self):
        store = Store(db_path=Path(tempfile.mktemp(suffix=".db")))
        return HeuristicForecaster(store), store

    def test_forecast_without_data(self):
        """Should work with fallback profile even with empty store."""
        forecaster, store = self._make_forecaster()
        fc = forecaster.forecast(hours=24)
        assert fc.forecast_hours == 24
        assert all(h.predicted_intensity.grams_co2_per_kwh > 0 for h in fc.hourly)
        store.close()

    def test_forecast_with_weather(self):
        """Weather corrections should shift the forecast."""
        forecaster, store = self._make_forecaster()
        now = datetime.now(EASTERN)

        # Hot weather should increase CI
        hot_weather = [
            WeatherSnapshot(
                timestamp=now + timedelta(hours=h),
                temperature_f=100,  # Very hot
                wind_speed_80m_mph=5,
                cloud_cover_pct=0,
            )
            for h in range(24)
        ]

        mild_weather = [
            WeatherSnapshot(
                timestamp=now + timedelta(hours=h),
                temperature_f=70,  # Comfortable
                wind_speed_80m_mph=20,  # Windy
                cloud_cover_pct=0,
            )
            for h in range(24)
        ]

        hot_fc = forecaster.forecast(hours=24, weather=hot_weather)
        mild_fc = forecaster.forecast(hours=24, weather=mild_weather)

        hot_avg = sum(h.predicted_intensity.grams_co2_per_kwh for h in hot_fc.hourly) / 24
        mild_avg = sum(h.predicted_intensity.grams_co2_per_kwh for h in mild_fc.hourly) / 24

        assert hot_avg > mild_avg  # Hot weather should produce higher CI
        store.close()

    def test_persistence_blend(self):
        """Near-term forecast should be influenced by current actual CI."""
        forecaster, store = self._make_forecaster()

        high_current = CarbonIntensity(grams_co2_per_kwh=500)
        fc = forecaster.forecast(hours=24, current_intensity=high_current)

        # First hour should be heavily influenced by current (500)
        # Later hours should revert toward baseline (~200-350)
        first_hour = fc.hourly[0].predicted_intensity.grams_co2_per_kwh
        last_hour = fc.hourly[-1].predicted_intensity.grams_co2_per_kwh
        assert first_hour > last_hour  # Persistence effect
        store.close()

    def test_forecast_max_48_hours(self):
        forecaster, store = self._make_forecaster()
        fc = forecaster.forecast(hours=100)  # Should be capped
        assert fc.forecast_hours == 48
        store.close()


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
