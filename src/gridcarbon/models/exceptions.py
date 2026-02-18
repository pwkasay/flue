"""Exception hierarchy for gridcarbon.

Follows the Cloverly pattern: Syntactic (bad format) vs Semantic (valid format,
invalid meaning). API layer maps these to 400 vs 422.
"""


class GridCarbonException(Exception):
    """Base exception for all gridcarbon errors."""

    pass


class SyntacticException(GridCarbonException):
    """Input is malformed (missing fields, wrong types)."""

    pass


class SemanticException(GridCarbonException):
    """Input is well-formed but meaningless (unknown fuel type, bad date range)."""

    pass


# ── Data Source Errors ──


class DataSourceError(GridCarbonException):
    """A data source is unavailable or returned unexpected data."""

    pass


class NYISOFetchError(DataSourceError):
    """Failed to fetch data from NYISO."""

    pass


class EIAFetchError(DataSourceError):
    """Failed to fetch data from EIA API."""

    pass


class WeatherFetchError(DataSourceError):
    """Failed to fetch weather data."""

    pass


# ── Domain Errors ──


class UnknownFuelCategory(SemanticException):
    """NYISO returned a fuel category we don't have an emission factor for."""

    pass


class InvalidDateRange(SemanticException):
    """Requested date range is invalid."""

    pass


class InsufficientHistoricalData(SemanticException):
    """Not enough historical data to generate a forecast."""

    pass


class StoreError(GridCarbonException):
    """Database read/write failure."""

    pass
