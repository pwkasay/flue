"""Emission factors for NYISO fuel categories.

These map NYISO's 7 fuel mix categories to grams of CO₂ per kilowatt-hour.
Factors are derived from EPA eGRID 2022 subregion data for NYCW/NYUP,
cross-referenced with EIA plant-level heat rates and NYSERDA's published
marginal emission factor of ~500 gCO₂/kWh.

Design decision: direct combustion factors (not lifecycle). This matches
EPA eGRID methodology and is the standard for Scope 2 accounting.

The NYISO fuel categories are:
- "Dual Fuel"        → NYC gas/oil plants, predominantly gas
- "Natural Gas"      → CCGT and simple cycle gas turbines
- "Nuclear"          → Indian Point replacement: Nine Mile Point, FitzPatrick
- "Other Fossil Fuels" → Oil, small coal remnants
- "Other Renewables" → Biomass, landfill gas, small solar
- "Wind"             → Onshore wind (growing offshore)
- "Hydro"            → Niagara, St. Lawrence, run-of-river
"""

from dataclasses import dataclass
from enum import Enum


class NYISOFuelCategory(str, Enum):
    """The 7 fuel categories NYISO reports in real-time fuel mix data."""

    DUAL_FUEL = "Dual Fuel"
    NATURAL_GAS = "Natural Gas"
    NUCLEAR = "Nuclear"
    OTHER_FOSSIL = "Other Fossil Fuels"
    OTHER_RENEWABLES = "Other Renewables"
    WIND = "Wind"
    HYDRO = "Hydro"

    @classmethod
    def from_nyiso_label(cls, label: str) -> NYISOFuelCategory:
        """Parse a fuel category from NYISO CSV data.

        Handles minor variations in labeling across different NYISO datasets.
        """
        normalized = label.strip().title()
        ALIASES = {
            "Dual Fuel": cls.DUAL_FUEL,
            "Natural Gas": cls.NATURAL_GAS,
            "Nuclear": cls.NUCLEAR,
            "Other Fossil Fuels": cls.OTHER_FOSSIL,
            "Other Fossil": cls.OTHER_FOSSIL,
            "Other Renewables": cls.OTHER_RENEWABLES,
            "Wind": cls.WIND,
            "Hydro": cls.HYDRO,
        }
        result = ALIASES.get(normalized)
        if result is None:
            from ..models.exceptions import UnknownFuelCategory

            raise UnknownFuelCategory(
                f"Unknown NYISO fuel category: '{label}'. "
                f"Known categories: {', '.join(c.value for c in cls)}"
            )
        return result


@dataclass(frozen=True)
class EmissionFactor:
    """Emission factor for a single fuel category.

    Attributes:
        fuel: The NYISO fuel category.
        grams_co2_per_kwh: Direct combustion CO₂ factor.
        source: Where this factor comes from (for transparency).
    """

    fuel: NYISOFuelCategory
    grams_co2_per_kwh: float
    source: str


# ── The Factor Registry ──
# These are the values your carbon intensity calculation will use.
# Adjust them as better data becomes available — the rest of the system
# is indifferent to the specific numbers.

EMISSION_FACTORS: dict[NYISOFuelCategory, EmissionFactor] = {
    NYISOFuelCategory.NATURAL_GAS: EmissionFactor(
        fuel=NYISOFuelCategory.NATURAL_GAS,
        grams_co2_per_kwh=450,
        source="EPA eGRID 2022 NYCW/NYUP weighted average for gas fleet",
    ),
    NYISOFuelCategory.DUAL_FUEL: EmissionFactor(
        fuel=NYISOFuelCategory.DUAL_FUEL,
        grams_co2_per_kwh=480,
        source="EPA eGRID 2022, NYC dual-fuel plants (predominantly gas operation)",
    ),
    NYISOFuelCategory.NUCLEAR: EmissionFactor(
        fuel=NYISOFuelCategory.NUCLEAR,
        grams_co2_per_kwh=0,
        source="Zero direct combustion emissions",
    ),
    NYISOFuelCategory.HYDRO: EmissionFactor(
        fuel=NYISOFuelCategory.HYDRO,
        grams_co2_per_kwh=0,
        source="Zero direct combustion emissions",
    ),
    NYISOFuelCategory.WIND: EmissionFactor(
        fuel=NYISOFuelCategory.WIND,
        grams_co2_per_kwh=0,
        source="Zero direct combustion emissions",
    ),
    NYISOFuelCategory.OTHER_RENEWABLES: EmissionFactor(
        fuel=NYISOFuelCategory.OTHER_RENEWABLES,
        grams_co2_per_kwh=0,
        source="Biomass/landfill gas treated as carbon-neutral by convention",
    ),
    NYISOFuelCategory.OTHER_FOSSIL: EmissionFactor(
        fuel=NYISOFuelCategory.OTHER_FOSSIL,
        grams_co2_per_kwh=840,
        source="EPA eGRID 2022 weighted average for oil/coal in NYISO",
    ),
}


def get_factor(fuel: NYISOFuelCategory) -> float:
    """Get the emission factor in gCO₂/kWh for a fuel category."""
    return EMISSION_FACTORS[fuel].grams_co2_per_kwh


def all_factors_summary() -> list[dict]:
    """Return a JSON-serializable summary of all emission factors."""
    return [
        {
            "fuel": ef.fuel.value,
            "grams_co2_per_kwh": ef.grams_co2_per_kwh,
            "source": ef.source,
        }
        for ef in EMISSION_FACTORS.values()
    ]
