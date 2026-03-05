"""Factories for boto3 clients/resources pointed to LocalStack."""

from __future__ import annotations

import boto3
from botocore.config import Config

from helsinki_pipeline.config import AppConfig


def _session(config: AppConfig) -> boto3.session.Session:
    """Create a boto3 session from typed project configuration."""

    return boto3.session.Session(
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        region_name=config.aws_region,
    )


def create_client(service_name: str, config: AppConfig):
    """Create a boto3 client for a LocalStack AWS service."""

    return _session(config).client(
        service_name,
        endpoint_url=config.localstack_endpoint,
        config=Config(retries={"max_attempts": 8, "mode": "standard"}),
    )


def create_resource(service_name: str, config: AppConfig):
    """Create a boto3 resource for a LocalStack AWS service."""

    return _session(config).resource(
        service_name,
        endpoint_url=config.localstack_endpoint,
        config=Config(retries={"max_attempts": 8, "mode": "standard"}),
    )
