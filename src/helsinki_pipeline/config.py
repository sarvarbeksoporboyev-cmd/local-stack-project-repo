"""Configuration helpers for local pipeline services."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class AppConfig:
    """Typed runtime configuration loaded from environment variables."""

    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str
    localstack_endpoint: str
    s3_bucket_name: str
    raw_prefix: str
    metrics_prefix: str
    sns_topic_name: str
    sqs_queue_name: str
    lambda_function_name: str
    lambda_role_arn: str
    dynamodb_raw_table: str
    dynamodb_period_metrics_table: str
    dynamodb_file_state_table: str

    @classmethod
    def from_env(
        cls,
        env_file: Path | None = None,
        use_docker_endpoint: bool = False,
    ) -> "AppConfig":
        """Build configuration from a local .env file and process environment."""

        if env_file is None:
            env_file = Path(".env")

        if env_file.exists():
            load_dotenv(env_file, override=False)

        local_endpoint = os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566")
        docker_endpoint = os.getenv("LOCALSTACK_ENDPOINT_DOCKER", local_endpoint)

        if use_docker_endpoint or Path("/.dockerenv").exists():
            endpoint = docker_endpoint
        else:
            endpoint = local_endpoint

        return cls(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            aws_region=os.getenv("AWS_REGION", "eu-north-1"),
            localstack_endpoint=endpoint,
            s3_bucket_name=os.getenv("S3_BUCKET_NAME", "helsinki-city-bikes"),
            raw_prefix=os.getenv("RAW_PREFIX", "raw/trips"),
            metrics_prefix=os.getenv("METRICS_PREFIX", "processed/station-metrics"),
            sns_topic_name=os.getenv("SNS_TOPIC_NAME", "helsinki-bike-file-topic"),
            sqs_queue_name=os.getenv("SQS_QUEUE_NAME", "helsinki-bike-file-queue"),
            lambda_function_name=os.getenv(
                "LAMBDA_FUNCTION_NAME",
                "helsinki_dynamodb_ingestor",
            ),
            lambda_role_arn=os.getenv(
                "LAMBDA_ROLE_ARN",
                "arn:aws:iam::000000000000:role/lambda-role",
            ),
            dynamodb_raw_table=os.getenv("DYNAMODB_RAW_TABLE", "bike_trips_raw"),
            dynamodb_period_metrics_table=os.getenv(
                "DYNAMODB_PERIOD_METRICS_TABLE",
                "bike_period_metrics",
            ),
            dynamodb_file_state_table=os.getenv(
                "DYNAMODB_FILE_STATE_TABLE",
                "bike_file_state",
            ),
        )
