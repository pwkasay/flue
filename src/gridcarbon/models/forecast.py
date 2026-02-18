"""Forecast models — the output of the heuristic forecaster.

A Forecast contains hourly CarbonIntensity predictions for a window
(typically 24–48 hours) plus derived recommendations: cleanest window,
dirtiest window, and when to schedule deferrable loads.
"""


from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from .fuel_mix import CarbonIntensity


@dataclass(frozen=True)
class HourlyForecast:
    """A single hourly forecast point."""

    hour: datetime
    predicted_intensity: CarbonIntensity
    confidence: str = "medium"  # low | medium | high

    def to_dict(self) -> dict[str, Any]:
        return {
            "hour": self.hour.isoformat(),
            "grams_co2_per_kwh": round(self.predicted_intensity.grams_co2_per_kwh, 1),
            "category": self.predicted_intensity.category,
            "label": self.predicted_intensity.category_label,
            "confidence": self.confidence,
        }


@dataclass
class ForecastWindow:
    """A time window identified as notable (cleanest, dirtiest, etc.)."""

    start: datetime
    end: datetime
    average_intensity: CarbonIntensity
    label: str  # "cleanest", "dirtiest", etc.

    @property
    def duration_hours(self) -> float:
        return (self.end - self.start).total_seconds() / 3600

    def to_dict(self) -> dict[str, Any]:
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "duration_hours": self.duration_hours,
            "avg_grams_co2_per_kwh": round(self.average_intensity.grams_co2_per_kwh, 1),
            "category": self.average_intensity.category,
            "label": self.label,
        }


@dataclass
class Forecast:
    """Complete forecast with hourly predictions and recommendations.

    The primary output object. Contains the raw hourly forecast plus
    derived insights: when is the cleanest/dirtiest window, and a
    plain-English summary.
    """

    generated_at: datetime
    hourly: list[HourlyForecast]
    region: str = "NYISO"

    @property
    def forecast_hours(self) -> int:
        return len(self.hourly)

    @property
    def start(self) -> datetime:
        return self.hourly[0].hour if self.hourly else self.generated_at

    @property
    def end(self) -> datetime:
        return self.hourly[-1].hour if self.hourly else self.generated_at

    def cleanest_window(self, window_hours: int = 3) -> ForecastWindow | None:
        """Find the N-hour window with the lowest average carbon intensity."""
        return self._find_window(window_hours, minimize=True)

    def dirtiest_window(self, window_hours: int = 3) -> ForecastWindow | None:
        """Find the N-hour window with the highest average carbon intensity."""
        return self._find_window(window_hours, minimize=False)

    def _find_window(
        self, window_hours: int, minimize: bool
    ) -> ForecastWindow | None:
        if len(self.hourly) < window_hours:
            return None

        best_avg = float("inf") if minimize else float("-inf")
        best_start = 0

        for i in range(len(self.hourly) - window_hours + 1):
            window = self.hourly[i : i + window_hours]
            avg = sum(h.predicted_intensity.grams_co2_per_kwh for h in window) / window_hours
            if (minimize and avg < best_avg) or (not minimize and avg > best_avg):
                best_avg = avg
                best_start = i

        window = self.hourly[best_start : best_start + window_hours]
        return ForecastWindow(
            start=window[0].hour,
            end=window[-1].hour + timedelta(hours=1),
            average_intensity=CarbonIntensity(grams_co2_per_kwh=best_avg),
            label="cleanest" if minimize else "dirtiest",
        )

    @property
    def summary(self) -> str:
        """Plain-English summary for CLI and notifications."""
        if not self.hourly:
            return "No forecast data available."

        current = self.hourly[0]
        cleanest = self.cleanest_window(3)
        dirtiest = self.dirtiest_window(3)

        lines = [
            f"Grid Carbon Forecast for {self.region}",
            f"Generated: {self.generated_at.strftime('%Y-%m-%d %H:%M %Z')}",
            f"",
            f"Right now: {current.predicted_intensity.grams_co2_per_kwh:.0f} gCO₂/kWh "
            f"{current.predicted_intensity.category_label}",
            f"  → {current.predicted_intensity.recommendation}",
        ]

        if cleanest:
            lines.extend([
                f"",
                f"Cleanest 3-hour window: {cleanest.start.strftime('%I:%M %p')} – "
                f"{cleanest.end.strftime('%I:%M %p')}",
                f"  → {cleanest.average_intensity.grams_co2_per_kwh:.0f} gCO₂/kWh "
                f"({cleanest.average_intensity.category})",
            ])

        if dirtiest:
            lines.extend([
                f"",
                f"Dirtiest 3-hour window: {dirtiest.start.strftime('%I:%M %p')} – "
                f"{dirtiest.end.strftime('%I:%M %p')}",
                f"  → {dirtiest.average_intensity.grams_co2_per_kwh:.0f} gCO₂/kWh "
                f"({dirtiest.average_intensity.category})",
            ])

        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        cleanest = self.cleanest_window(3)
        dirtiest = self.dirtiest_window(3)
        return {
            "region": self.region,
            "generated_at": self.generated_at.isoformat(),
            "forecast_hours": self.forecast_hours,
            "hourly": [h.to_dict() for h in self.hourly],
            "cleanest_3h_window": cleanest.to_dict() if cleanest else None,
            "dirtiest_3h_window": dirtiest.to_dict() if dirtiest else None,
        }
