"""Open-Meteo weather data source.

Provides temperature, wind speed, and cloud cover data for NYC.
No API key required. Free tier: 10,000 requests/day.

Weather variables that affect NYISO carbon intensity:
- Temperature: drives heating (winter) and cooling (summer) demand
- Wind speed at hub height (80m): determines wind generation output
- Cloud cover: affects behind-the-meter solar contribution
"""

import logging
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

import httpx

from ..models.exceptions import WeatherFetchError

logger = logging.getLogger("gridcarbon.sources.weather")

# NYC coordinates
NYC_LAT = 40.71
NYC_LON = -74.01
EASTERN = ZoneInfo("America/New_York")

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
HISTORICAL_URL = "https://archive-api.open-meteo.com/v1/archive"


@dataclass(frozen=True)
class WeatherSnapshot:
    """Hourly weather observation relevant to grid carbon intensity."""

    timestamp: datetime
    temperature_f: float
    wind_speed_80m_mph: float
    cloud_cover_pct: float

    @property
    def temperature_c(self) -> float:
        return (self.temperature_f - 32) * 5 / 9

    @property
    def is_heating_weather(self) -> bool:
        """Below ~65°F, heating demand increases gas consumption."""
        return self.temperature_f < 65

    @property
    def is_cooling_weather(self) -> bool:
        """Above ~65°F, cooling demand increases gas consumption."""
        return self.temperature_f > 75

    @property
    def temperature_departure_from_comfort(self) -> float:
        """Degrees away from the 65–75°F comfort zone. Always >= 0."""
        if self.temperature_f < 65:
            return 65 - self.temperature_f
        elif self.temperature_f > 75:
            return self.temperature_f - 75
        return 0.0


def _parse_hourly_response(data: dict) -> list[WeatherSnapshot]:
    """Parse Open-Meteo hourly response into WeatherSnapshot list."""
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    winds = hourly.get("wind_speed_80m", [])
    clouds = hourly.get("cloud_cover", [])

    snapshots = []
    for i, ts_str in enumerate(times):
        try:
            ts = datetime.fromisoformat(ts_str).replace(tzinfo=EASTERN)
            # Open-Meteo returns Celsius by default; convert to F
            temp_c = temps[i] if i < len(temps) else 0
            temp_f = temp_c * 9 / 5 + 32
            # Wind comes in km/h; convert to mph
            wind_kmh = winds[i] if i < len(winds) else 0
            wind_mph = wind_kmh * 0.621371
            cloud = clouds[i] if i < len(clouds) else 0

            snapshots.append(
                WeatherSnapshot(
                    timestamp=ts,
                    temperature_f=round(temp_f, 1),
                    wind_speed_80m_mph=round(wind_mph, 1),
                    cloud_cover_pct=round(cloud, 1),
                )
            )
        except (ValueError, IndexError) as e:
            logger.debug("Skipping weather data point %d: %s", i, e)
            continue

    return snapshots


async def fetch_forecast(
    days: int = 2,
    lat: float = NYC_LAT,
    lon: float = NYC_LON,
) -> list[WeatherSnapshot]:
    """Fetch weather forecast for the next N days."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,wind_speed_80m,cloud_cover",
        "forecast_days": days,
        "timezone": "America/New_York",
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.get(FORECAST_URL, params=params)
            resp.raise_for_status()
            return _parse_hourly_response(resp.json())
        except (httpx.HTTPError, KeyError) as e:
            raise WeatherFetchError(f"Weather forecast fetch failed: {e}") from e


async def fetch_historical(
    start: date,
    end: date,
    lat: float = NYC_LAT,
    lon: float = NYC_LON,
) -> list[WeatherSnapshot]:
    """Fetch historical weather data for a date range."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": "temperature_2m,wind_speed_80m,cloud_cover",
        "timezone": "America/New_York",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.get(HISTORICAL_URL, params=params)
            resp.raise_for_status()
            return _parse_hourly_response(resp.json())
        except (httpx.HTTPError, KeyError) as e:
            raise WeatherFetchError(f"Historical weather fetch failed: {e}") from e
