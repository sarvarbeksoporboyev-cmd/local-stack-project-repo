"""Tests for shared column alias resolution."""

from helsinki_pipeline.etl.column_mapping import resolve_column


def test_resolve_column_case_insensitive() -> None:
    columns = ["Departure station name", "Duration (sec.)"]
    resolved = resolve_column(columns, ["departure station name", "departure_name"])
    assert resolved == "Departure station name"


def test_resolve_column_raises_when_missing() -> None:
    columns = ["a", "b"]
    try:
        resolve_column(columns, ["missing_col"])
    except KeyError:
        assert True
    else:
        assert False, "Expected KeyError for unknown alias"
