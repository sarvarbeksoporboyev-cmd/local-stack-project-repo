"""DAG to ingest one CSV, compute station metrics with Spark, and upload both outputs."""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from helsinki_pipeline.airflow.tasks import (
    create_station_metrics_file,
    upload_metrics_file,
    upload_single_raw_file,
)


@dag(
    dag_id="single_file_to_s3_with_spark_metrics",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "input_file": Param(
            "2020-06.csv",
            type="string",
            description="File name in data/input_dropzone or absolute path.",
        )
    },
    tags=["localstack", "spark", "s3", "metrics"],
)
def single_file_to_s3_with_spark_metrics_dag():
    """Upload one CSV to S3, create Spark metrics, and upload metrics CSV."""

    @task
    def upload_raw() -> str:
        context = get_current_context()
        input_file = context["params"]["input_file"]
        return upload_single_raw_file(input_file)

    @task
    def run_spark_metrics() -> str:
        context = get_current_context()
        input_file = context["params"]["input_file"]
        return create_station_metrics_file(input_file)

    @task
    def upload_metrics(metrics_file: str) -> str:
        context = get_current_context()
        input_file = context["params"]["input_file"]
        return upload_metrics_file(input_file, metrics_file)

    raw_key = upload_raw()
    metrics_file_path = run_spark_metrics()
    metrics_key = upload_metrics(metrics_file_path)

    raw_key >> metrics_file_path >> metrics_key


dag = single_file_to_s3_with_spark_metrics_dag()
