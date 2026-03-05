"""Column alias mappings shared by ETL and Spark jobs."""

from __future__ import annotations

from typing import Iterable

DEPARTURE_TIME_ALIASES = [
    "Departure",
    "departure",
    "departure_time",
    "departed_at",
    "started_at",
]

DEPARTURE_STATION_ALIASES = [
    "departure_name",
    "Departure station name",
    "Departure station",
    "from_station_name",
]

RETURN_STATION_ALIASES = [
    "return_name",
    "Return station name",
    "Return station",
    "to_station_name",
]

DISTANCE_ALIASES = [
    "distance_m",
    "Covered distance (m)",
    "Distance",
    "distance",
]

DURATION_ALIASES = [
    "duration_sec",
    "Duration (sec.)",
    "Duration",
    "duration",
]

TEMPERATURE_ALIASES = [
    "temperature",
    "Air temperature (degC)",
    "temperature_c",
    "Temp",
]


def resolve_column(columns: Iterable[str], aliases: list[str]) -> str:
    """Resolve a real column name by matching aliases case-insensitively."""

    normalized = {col.strip().lower(): col for col in columns}
    for alias in aliases:
        match = normalized.get(alias.strip().lower())
        if match:
            return match

    alias_list = ", ".join(aliases)
    raise KeyError(f"None of the expected aliases found: {alias_list}")
