"""Lambda handler to wait for raw+metrics file pairs and load DynamoDB analytics."""

from __future__ import annotations

import hashlib
import io
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import unquote_plus

import boto3
import pandas as pd


DEPARTURE_TIME_ALIASES = [
    "Departure",
    "departure",
    "departure_time",
    "departed_at",
    "started_at",
]
RETURN_TIME_ALIASES = [
    "Return",
    "return",
    "return_time",
    "ended_at",
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


@dataclass(frozen=True)
class LambdaSettings:
    """Runtime settings for LocalStack Lambda execution."""

    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str
    localstack_endpoint: str
    s3_bucket_name: str
    raw_prefix: str
    metrics_prefix: str
    raw_table: str
    period_metrics_table: str
    file_state_table: str

    @classmethod
    def from_env(cls) -> "LambdaSettings":
        """Load settings from environment variables."""

        return cls(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            aws_region=os.getenv("AWS_REGION", "eu-north-1"),
            localstack_endpoint=os.getenv("LOCALSTACK_ENDPOINT", "http://localstack:4566"),
            s3_bucket_name=os.getenv("S3_BUCKET_NAME", "helsinki-city-bikes"),
            raw_prefix=os.getenv("RAW_PREFIX", "raw/trips"),
            metrics_prefix=os.getenv("METRICS_PREFIX", "processed/station-metrics"),
            raw_table=os.getenv("DYNAMODB_RAW_TABLE", "bike_trips_raw"),
            period_metrics_table=os.getenv("DYNAMODB_PERIOD_METRICS_TABLE", "bike_period_metrics"),
            file_state_table=os.getenv("DYNAMODB_FILE_STATE_TABLE", "bike_file_state"),
        )


class AwsRuntime:
    """AWS clients/resources bound to LocalStack endpoint."""

    def __init__(self, settings: LambdaSettings) -> None:
        session = boto3.session.Session(
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )
        self.s3 = session.client("s3", endpoint_url=settings.localstack_endpoint)
        self.dynamodb = session.resource("dynamodb", endpoint_url=settings.localstack_endpoint)


class FileStateRepository:
    """Track whether both raw and metrics file for a period are available."""

    def __init__(self, table) -> None:
        self.table = table

    def upsert_reference(self, file_id: str, key: str, file_type: str) -> dict[str, Any]:
        """Store raw/metrics file reference and return latest state row."""

        existing = self.table.get_item(Key={"file_id": file_id}).get("Item", {})
        item = {
            "file_id": file_id,
            "raw_key": existing.get("raw_key"),
            "metrics_key": existing.get("metrics_key"),
            "processed": existing.get("processed", False),
            "updated_at": _utc_now_iso(),
        }

        if file_type == "raw":
            item["raw_key"] = key
        elif file_type == "metrics":
            item["metrics_key"] = key
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        self.table.put_item(Item=item)
        return item

    def mark_processed(self, file_id: str) -> None:
        """Mark a file pair as consumed to keep Lambda idempotent."""

        self.table.update_item(
            Key={"file_id": file_id},
            UpdateExpression="SET processed = :processed, processed_at = :processed_at",
            ExpressionAttributeValues={
                ":processed": True,
                ":processed_at": _utc_now_iso(),
            },
        )


class S3CsvReader:
    """Read CSV files directly from S3 into pandas dataframes."""

    def __init__(self, s3_client, bucket_name: str) -> None:
        self._s3 = s3_client
        self.bucket_name = bucket_name

    def read_csv(self, key: str) -> pd.DataFrame:
        """Read one CSV object from S3."""

        response = self._s3.get_object(Bucket=self.bucket_name, Key=key)
        body = response["Body"].read()
        return pd.read_csv(io.BytesIO(body), low_memory=False)


class BikeDataTransformer:
    """Normalize bike ride data and compute daily/monthly metrics."""

    @staticmethod
    def normalize(raw_frame: pd.DataFrame) -> pd.DataFrame:
        """Normalize source frame into a consistent schema for downstream writes."""

        departure_col = _resolve_column(raw_frame.columns, DEPARTURE_TIME_ALIASES)
        return_col = _resolve_column(raw_frame.columns, RETURN_TIME_ALIASES, required=False)
        departure_station_col = _resolve_column(raw_frame.columns, DEPARTURE_STATION_ALIASES)
        return_station_col = _resolve_column(raw_frame.columns, RETURN_STATION_ALIASES)

        distance_col = _resolve_column(raw_frame.columns, DISTANCE_ALIASES, required=False)
        duration_col = _resolve_column(raw_frame.columns, DURATION_ALIASES, required=False)
        temperature_col = _resolve_column(raw_frame.columns, TEMPERATURE_ALIASES, required=False)

        normalized = pd.DataFrame()
        normalized["departure_ts"] = pd.to_datetime(raw_frame[departure_col], errors="coerce")

        if return_col:
            normalized["return_ts"] = pd.to_datetime(raw_frame[return_col], errors="coerce")
        else:
            normalized["return_ts"] = pd.NaT

        normalized["departure_name"] = raw_frame[departure_station_col].astype(str)
        normalized["return_name"] = raw_frame[return_station_col].astype(str)

        normalized["distance_m"] = _to_numeric_column(raw_frame, distance_col)
        normalized["duration_sec"] = _to_numeric_column(raw_frame, duration_col)
        normalized["temperature_c"] = _to_numeric_column(raw_frame, temperature_col)

        normalized = normalized.dropna(subset=["departure_ts"]).reset_index(drop=True)
        normalized["distance_m"] = normalized["distance_m"].fillna(0.0)
        normalized["duration_sec"] = normalized["duration_sec"].fillna(0.0)
        normalized["temperature_c"] = normalized["temperature_c"].fillna(0.0)

        duration_non_zero = normalized["duration_sec"].replace(0.0, pd.NA)
        normalized["speed_kmh"] = (normalized["distance_m"] / duration_non_zero) * 3.6
        normalized["speed_kmh"] = normalized["speed_kmh"].fillna(0.0)

        return normalized

    @staticmethod
    def compute_period_metrics(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Compute day-level and month-level aggregates required by the task."""

        work = frame.copy()
        work["period_day"] = work["departure_ts"].dt.strftime("%Y-%m-%d")
        work["period_month"] = work["departure_ts"].dt.to_period("M").astype(str)

        metric_columns = {
            "avg_distance_m": ("distance_m", "mean"),
            "avg_duration_sec": ("duration_sec", "mean"),
            "avg_speed_kmh": ("speed_kmh", "mean"),
            "avg_temperature_c": ("temperature_c", "mean"),
        }

        daily = (
            work.groupby("period_day", as_index=False)
            .agg(**metric_columns)
            .rename(columns={"period_day": "period_value"})
        )

        monthly = (
            work.groupby("period_month", as_index=False)
            .agg(**metric_columns)
            .rename(columns={"period_month": "period_value"})
        )

        return daily, monthly


class DynamoWriter:
    """Write normalized rows and metric aggregates to DynamoDB."""

    def __init__(self, raw_table, period_table) -> None:
        self.raw_table = raw_table
        self.period_table = period_table

    def write_raw_rows(
        self,
        frame: pd.DataFrame,
        file_id: str,
        raw_key: str,
        metrics_key: str,
    ) -> int:
        """Write normalized trip rows to raw DynamoDB table."""

        written = 0
        with self.raw_table.batch_writer(overwrite_by_pkeys=["trip_id"]) as batch:
            for index, row in frame.iterrows():
                trip_id = _build_trip_id(file_id, index, row)
                item = {
                    "trip_id": trip_id,
                    "file_id": file_id,
                    "raw_key": raw_key,
                    "metrics_key": metrics_key,
                    "departure_ts": row["departure_ts"].isoformat(),
                    "departure_name": str(row["departure_name"]),
                    "return_name": str(row["return_name"]),
                    "distance_m": _to_decimal(row["distance_m"]),
                    "duration_sec": _to_decimal(row["duration_sec"]),
                    "speed_kmh": _to_decimal(row["speed_kmh"]),
                    "temperature_c": _to_decimal(row["temperature_c"]),
                    "created_at": _utc_now_iso(),
                }

                if pd.notna(row["return_ts"]):
                    item["return_ts"] = row["return_ts"].isoformat()

                batch.put_item(Item=_drop_none(item))
                written += 1

        return written

    def write_period_metrics(self, frame: pd.DataFrame, period_type: str, file_id: str) -> int:
        """Write day/month aggregate rows to DynamoDB."""

        written = 0
        for _, row in frame.iterrows():
            item = {
                "period_type": period_type,
                "period_value": str(row["period_value"]),
                "source_file_id": file_id,
                "avg_distance_m": _to_decimal(row["avg_distance_m"]),
                "avg_duration_sec": _to_decimal(row["avg_duration_sec"]),
                "avg_speed_kmh": _to_decimal(row["avg_speed_kmh"]),
                "avg_temperature_c": _to_decimal(row["avg_temperature_c"]),
                "updated_at": _utc_now_iso(),
            }
            self.period_table.put_item(Item=_drop_none(item))
            written += 1

        return written


class PairIngestionService:
    """Orchestrate file pair checking and DynamoDB loading logic."""

    def __init__(self, settings: LambdaSettings, aws_runtime: AwsRuntime) -> None:
        self.settings = settings
        self.reader = S3CsvReader(aws_runtime.s3, settings.s3_bucket_name)
        self.state_repo = FileStateRepository(
            aws_runtime.dynamodb.Table(settings.file_state_table),
        )
        self.writer = DynamoWriter(
            aws_runtime.dynamodb.Table(settings.raw_table),
            aws_runtime.dynamodb.Table(settings.period_metrics_table),
        )

    def handle_event(self, event: dict[str, Any]) -> dict[str, Any]:
        """Process SQS event records and trigger pair ingestion when ready."""

        processed_file_ids: list[str] = []

        for bucket, key in _extract_s3_keys(event):
            if bucket != self.settings.s3_bucket_name:
                continue

            file_type = _classify_file_type(
                key=key,
                raw_prefix=self.settings.raw_prefix,
                metrics_prefix=self.settings.metrics_prefix,
            )
            if file_type is None:
                continue

            file_id = _extract_file_id(key)
            state = self.state_repo.upsert_reference(file_id=file_id, key=key, file_type=file_type)

            if state.get("processed"):
                continue

            raw_key = state.get("raw_key")
            metrics_key = state.get("metrics_key")
            if not raw_key or not metrics_key:
                continue

            self._process_pair(file_id=file_id, raw_key=raw_key, metrics_key=metrics_key)
            self.state_repo.mark_processed(file_id)
            processed_file_ids.append(file_id)

        return {
            "processed_file_ids": processed_file_ids,
            "processed_count": len(processed_file_ids),
        }

    def _process_pair(self, file_id: str, raw_key: str, metrics_key: str) -> None:
        """Read files from S3 and write raw plus aggregate outputs to DynamoDB."""

        raw_frame = self.reader.read_csv(raw_key)
        metrics_frame = self.reader.read_csv(metrics_key)
        if metrics_frame.empty:
            raise ValueError(f"Metrics file is empty: {metrics_key}")

        normalized = BikeDataTransformer.normalize(raw_frame)
        daily_metrics, monthly_metrics = BikeDataTransformer.compute_period_metrics(normalized)

        self.writer.write_raw_rows(
            frame=normalized,
            file_id=file_id,
            raw_key=raw_key,
            metrics_key=metrics_key,
        )
        self.writer.write_period_metrics(daily_metrics, period_type="day", file_id=file_id)
        self.writer.write_period_metrics(monthly_metrics, period_type="month", file_id=file_id)


def lambda_handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """Lambda entrypoint for SQS-driven ingestion flow."""

    settings = LambdaSettings.from_env()
    runtime = AwsRuntime(settings)
    service = PairIngestionService(settings, runtime)
    return service.handle_event(event)


def _extract_s3_keys(event: dict[str, Any]) -> Iterable[tuple[str, str]]:
    """Extract (bucket, key) tuples from SQS records with SNS-wrapped S3 events."""

    for record in event.get("Records", []):
        body = json.loads(record.get("body", "{}"))

        if "Message" in body:
            payload = json.loads(body["Message"])
        else:
            payload = body

        for s3_record in payload.get("Records", []):
            bucket_name = s3_record["s3"]["bucket"]["name"]
            object_key = unquote_plus(s3_record["s3"]["object"]["key"])
            yield bucket_name, object_key


def _classify_file_type(key: str, raw_prefix: str, metrics_prefix: str) -> str | None:
    """Determine if the S3 key is raw or metrics object for this pipeline."""

    normalized = key.strip("/")
    if normalized.startswith(raw_prefix.strip("/") + "/"):
        return "raw"
    if normalized.startswith(metrics_prefix.strip("/") + "/"):
        return "metrics"
    return None


def _extract_file_id(key: str) -> str:
    """Build a deterministic file id from a raw or metrics file key."""

    file_name = Path(key).name
    if file_name.endswith("_station_metrics.csv"):
        return file_name[: -len("_station_metrics.csv")]
    if file_name.endswith(".csv"):
        return file_name[:-4]
    return file_name


def _resolve_column(columns: Iterable[str], aliases: list[str], required: bool = True) -> str | None:
    """Resolve source column by alias with case-insensitive matching."""

    normalized = {col.strip().lower(): col for col in columns}
    for alias in aliases:
        match = normalized.get(alias.strip().lower())
        if match:
            return match

    if required:
        options = ", ".join(aliases)
        raise KeyError(f"Required column was not found. Expected one of: {options}")

    return None


def _to_numeric_column(frame: pd.DataFrame, column: str | None) -> pd.Series:
    """Safely convert optional source column to numeric series."""

    if column is None:
        return pd.Series([0.0] * len(frame), index=frame.index)
    return pd.to_numeric(frame[column], errors="coerce")


def _build_trip_id(file_id: str, index: int, row: pd.Series) -> str:
    """Create a stable unique trip id from file and row attributes."""

    source = "|".join(
        [
            file_id,
            str(index),
            str(row.get("departure_ts", "")),
            str(row.get("departure_name", "")),
            str(row.get("return_name", "")),
        ]
    )
    return hashlib.sha1(source.encode("utf-8")).hexdigest()


def _to_decimal(value: Any) -> Decimal | None:
    """Convert numeric values to Decimal for DynamoDB compatibility."""

    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    try:
        return Decimal(str(round(float(value), 6)))
    except (TypeError, ValueError):
        return None


def _drop_none(item: dict[str, Any]) -> dict[str, Any]:
    """Remove None values from an item before writing to DynamoDB."""

    return {key: value for key, value in item.items() if value is not None}


def _utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""

    return datetime.now(timezone.utc).isoformat()
