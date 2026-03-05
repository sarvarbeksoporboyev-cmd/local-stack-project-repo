"""S3 upload service for monthly bike CSV files."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

from helsinki_pipeline.aws.aws_clients import create_client
from helsinki_pipeline.config import AppConfig


MONTH_PATTERN = re.compile(r"(?P<year>\d{4})-(?P<month>\d{2})")


class S3Uploader:
    """Upload bike CSV files to S3 with consistent partition-like paths."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._s3 = create_client("s3", config)

    def upload_file(self, local_file: Path, s3_key: str) -> str:
        """Upload one local file to S3 and return the destination key."""

        self._s3.upload_file(str(local_file), self.config.s3_bucket_name, s3_key)
        return s3_key

    def list_csv_files(self, directory: Path) -> Iterable[Path]:
        """Yield CSV files from a directory in deterministic order."""

        return sorted(directory.glob("*.csv"))

    def build_raw_key(self, local_file: Path) -> str:
        """Build an S3 key for a raw trip file, partitioned by month when possible."""

        match = MONTH_PATTERN.search(local_file.stem)
        if match:
            year = match.group("year")
            month = match.group("month")
            return (
                f"{self.config.raw_prefix}/year={year}/month={month}/"
                f"{local_file.name}"
            )

        return f"{self.config.raw_prefix}/unclassified/{local_file.name}"

    def build_metrics_key(self, source_file: Path, metrics_file: Path) -> str:
        """Build an S3 key for station metric output tied to a source file."""

        match = MONTH_PATTERN.search(source_file.stem)
        if match:
            year = match.group("year")
            month = match.group("month")
            return (
                f"{self.config.metrics_prefix}/year={year}/month={month}/"
                f"{metrics_file.name}"
            )

        return f"{self.config.metrics_prefix}/unclassified/{metrics_file.name}"
