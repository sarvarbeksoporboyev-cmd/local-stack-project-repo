"""DAG to upload all CSV files from a local dropzone to S3."""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from helsinki_pipeline.airflow.tasks import upload_directory_to_s3


@dag(
    dag_id="bulk_upload_dropzone_to_s3",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["localstack", "s3", "bulk-upload"],
)
def bulk_upload_dropzone_to_s3_dag():
    """Upload all available CSV files from the dropzone folder to S3."""

    @task
    def upload_files() -> list[str]:
        return upload_directory_to_s3()

    upload_files()


dag = bulk_upload_dropzone_to_s3_dag()
