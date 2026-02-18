"""Domain models for fuel mix snapshots and carbon intensity.

Follows the Cloverly unit-class pattern: canonical internal representation,
named properties for conversions, composable via operators.
"""


from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ..sources.emission_factors import (
    EMISSION_FACTORS,
    NYISOFuelCategory,
    get_factor,
)


@dataclass(frozen=True)
class FuelGeneration:
    """Generation from a single fuel category at a point in time.

    This is one row from NYISO's rtfuelmix CSV.
    """

    fuel: NYISOFuelCategory
    generation_mw: float

    @property
    def is_clean(self) -> bool:
        return get_factor(self.fuel) == 0

    @property
    def is_fossil(self) -> bool:
        return not self.is_clean


@dataclass
class FuelMix:
    """A complete fuel mix snapshot — all fuel categories at a single timestamp.

    This is the fundamental data unit. One FuelMix = one 5-minute interval
    from NYISO, containing generation (MW) for each of the 7 fuel categories.

    The carbon intensity is computed eagerly at construction (following the
    Cloverly pattern of "compute at init for domain objects with a single output").
    """

    timestamp: datetime
    fuels: list[FuelGeneration]
    timezone_label: str = "US/Eastern"

    # Computed at init
    _carbon_intensity: CarbonIntensity | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.fuels:
            self._carbon_intensity = self._calculate_intensity()

    def _calculate_intensity(self) -> CarbonIntensity:
        """Average carbon intensity: Σ(gen × factor) / Σ(gen)."""
        total_gen = sum(f.generation_mw for f in self.fuels)
        if total_gen <= 0:
            return CarbonIntensity(grams_co2_per_kwh=0.0, timestamp=self.timestamp)

        weighted_emissions = sum(
            f.generation_mw * get_factor(f.fuel) for f in self.fuels
        )
        ci = weighted_emissions / total_gen
        return CarbonIntensity(grams_co2_per_kwh=ci, timestamp=self.timestamp)

    @property
    def carbon_intensity(self) -> CarbonIntensity:
        if self._carbon_intensity is None:
            raise ValueError("FuelMix has no fuel data")
        return self._carbon_intensity

    @property
    def total_generation_mw(self) -> float:
        return sum(f.generation_mw for f in self.fuels)

    @property
    def clean_generation_mw(self) -> float:
        return sum(f.generation_mw for f in self.fuels if f.is_clean)

    @property
    def fossil_generation_mw(self) -> float:
        return sum(f.generation_mw for f in self.fuels if f.is_fossil)

    @property
    def clean_percentage(self) -> float:
        total = self.total_generation_mw
        if total <= 0:
            return 0.0
        return (self.clean_generation_mw / total) * 100

    @property
    def fuel_breakdown(self) -> dict[str, float]:
        """Fuel category → MW, sorted by generation descending."""
        return {
            f.fuel.value: f.generation_mw
            for f in sorted(self.fuels, key=lambda x: x.generation_mw, reverse=True)
        }

    @property
    def fuel_percentages(self) -> dict[str, float]:
        """Fuel category → percentage of total generation."""
        total = self.total_generation_mw
        if total <= 0:
            return {}
        return {
            f.fuel.value: round((f.generation_mw / total) * 100, 1)
            for f in sorted(self.fuels, key=lambda x: x.generation_mw, reverse=True)
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "carbon_intensity_gco2_kwh": round(self.carbon_intensity.grams_co2_per_kwh, 1),
            "total_generation_mw": round(self.total_generation_mw, 1),
            "clean_percentage": round(self.clean_percentage, 1),
            "fuel_breakdown_mw": {k: round(v, 1) for k, v in self.fuel_breakdown.items()},
        }


@dataclass(frozen=True)
class CarbonIntensity:
    """Carbon intensity at a point in time.

    Canonical unit: grams CO₂ per kilowatt-hour (gCO₂/kWh).

    Following the Cloverly unit-class pattern: single canonical internal unit,
    named properties for conversions.
    """

    grams_co2_per_kwh: float
    timestamp: datetime | None = None

    # ── Conversions ──

    @property
    def kg_co2_per_kwh(self) -> float:
        return self.grams_co2_per_kwh / 1000

    @property
    def kg_co2_per_mwh(self) -> float:
        return self.grams_co2_per_kwh

    @property
    def lbs_co2_per_mwh(self) -> float:
        return self.kg_co2_per_mwh * 2.20462

    @property
    def tons_co2_per_mwh(self) -> float:
        return self.lbs_co2_per_mwh / 2000

    # ── Classification ──

    @property
    def category(self) -> str:
        """Human-readable category for the current intensity level.

        Thresholds calibrated for NYISO's typical range of ~100-450 gCO₂/kWh.
        """
        g = self.grams_co2_per_kwh
        if g <= 150:
            return "very_clean"
        elif g <= 250:
            return "clean"
        elif g <= 350:
            return "moderate"
        elif g <= 450:
            return "dirty"
        else:
            return "very_dirty"

    @property
    def category_label(self) -> str:
        LABELS = {
            "very_clean": "🟢 Very Clean",
            "clean": "🟢 Clean",
            "moderate": "🟡 Moderate",
            "dirty": "🟠 Dirty",
            "very_dirty": "🔴 Very Dirty",
        }
        return LABELS[self.category]

    @property
    def recommendation(self) -> str:
        """Plain-English recommendation for load shifting."""
        RECS = {
            "very_clean": "Great time to run energy-intensive tasks!",
            "clean": "Good time for discretionary electricity use.",
            "moderate": "Grid is average right now. Defer if you can wait a few hours.",
            "dirty": "Consider waiting — the grid is carbon-heavy right now.",
            "very_dirty": "Worst time for electricity use. Defer everything you can.",
        }
        return RECS[self.category]

    # ── Operators (composable à la Cloverly) ──

    def __add__(self, other: CarbonIntensity) -> CarbonIntensity:
        """Sum (for accumulation before averaging)."""
        return CarbonIntensity(
            grams_co2_per_kwh=self.grams_co2_per_kwh + other.grams_co2_per_kwh
        )

    def __truediv__(self, other: int | float) -> CarbonIntensity:
        """Divide (for averaging)."""
        return CarbonIntensity(
            grams_co2_per_kwh=self.grams_co2_per_kwh / other
        )

    def __lt__(self, other: CarbonIntensity) -> bool:
        return self.grams_co2_per_kwh < other.grams_co2_per_kwh

    def __le__(self, other: CarbonIntensity) -> bool:
        return self.grams_co2_per_kwh <= other.grams_co2_per_kwh

    def __repr__(self) -> str:
        ts = self.timestamp.strftime("%H:%M") if self.timestamp else "?"
        return f"<CI {self.grams_co2_per_kwh:.0f} gCO₂/kWh @ {ts} [{self.category}]>"
