"""Business logic used by Airflow DAGs for S3 uploads and Spark metrics."""

from __future__ import annotations

from pathlib import Path

from airflow.exceptions import AirflowSkipException

from helsinki_pipeline.aws.s3_uploader import S3Uploader
from helsinki_pipeline.config import AppConfig
from helsinki_pipeline.spark.station_metrics_job import run_station_metrics_job

PROJECT_DATA_DIR = Path("/opt/project/data")
DROPZONE_DIR = PROJECT_DATA_DIR / "input_dropzone"
SPARK_OUTPUT_DIR = PROJECT_DATA_DIR / "spark_output"


def _config() -> AppConfig:
    return AppConfig.from_env(Path("/opt/project/.env"), use_docker_endpoint=True)


def upload_directory_to_s3(directory: Path = DROPZONE_DIR) -> list[str]:
    """Upload all CSV files in a directory to S3 as raw files."""

    cfg = _config()
    uploader = S3Uploader(cfg)
    csv_files = list(uploader.list_csv_files(directory))
    if not csv_files:
        raise AirflowSkipException(f"No CSV files found in {directory}")

    uploaded_keys: list[str] = []
    for csv_file in csv_files:
        key = uploader.build_raw_key(csv_file)
        uploader.upload_file(csv_file, key)
        uploaded_keys.append(key)

    return uploaded_keys


def resolve_input_file(input_file: str) -> Path:
    """Resolve a DAG input filename to an absolute path in the dropzone."""

    candidate = Path(input_file)
    if not candidate.is_absolute():
        candidate = DROPZONE_DIR / candidate

    if not candidate.exists():
        raise FileNotFoundError(f"Input CSV does not exist: {candidate}")

    return candidate


def upload_single_raw_file(input_file: str) -> str:
    """Upload one raw CSV file to S3."""

    cfg = _config()
    uploader = S3Uploader(cfg)
    source_file = resolve_input_file(input_file)
    key = uploader.build_raw_key(source_file)
    return uploader.upload_file(source_file, key)


def create_station_metrics_file(input_file: str) -> str:
    """Run Spark metrics job and return generated metrics file path."""

    source_file = resolve_input_file(input_file)
    metrics_name = f"{source_file.stem}_station_metrics.csv"
    output_path = SPARK_OUTPUT_DIR / metrics_name
    run_station_metrics_job(source_file, output_path)
    return str(output_path)


def upload_metrics_file(input_file: str, metrics_file: str) -> str:
    """Upload Spark-generated metrics CSV to S3."""

    cfg = _config()
    uploader = S3Uploader(cfg)
    source_file = resolve_input_file(input_file)
    metrics_path = Path(metrics_file)
    key = uploader.build_metrics_key(source_file, metrics_path)
    return uploader.upload_file(metrics_path, key)
