"""Tests for S3 key naming conventions."""

from pathlib import Path

from helsinki_pipeline.aws.s3_uploader import S3Uploader
from helsinki_pipeline.config import AppConfig


def build_config() -> AppConfig:
    return AppConfig(
        aws_access_key_id="test",
        aws_secret_access_key="test",
        aws_region="eu-north-1",
        localstack_endpoint="http://localhost:4566",
        s3_bucket_name="bucket",
        raw_prefix="raw/trips",
        metrics_prefix="processed/station-metrics",
        sns_topic_name="topic",
        sqs_queue_name="queue",
        lambda_function_name="lambda",
        lambda_role_arn="arn:aws:iam::000000000000:role/lambda-role",
        dynamodb_raw_table="raw",
        dynamodb_period_metrics_table="period",
        dynamodb_file_state_table="state",
    )


def test_build_raw_key_partitioned() -> None:
    uploader = S3Uploader(build_config())
    key = uploader.build_raw_key(Path("2020-11.csv"))
    assert key == "raw/trips/year=2020/month=11/2020-11.csv"


def test_build_metrics_key_partitioned() -> None:
    uploader = S3Uploader(build_config())
    key = uploader.build_metrics_key(
        source_file=Path("2019-03.csv"),
        metrics_file=Path("2019-03_station_metrics.csv"),
    )
    assert key == (
        "processed/station-metrics/year=2019/month=03/"
        "2019-03_station_metrics.csv"
    )
