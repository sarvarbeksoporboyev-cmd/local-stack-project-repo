"""Spark job to compute departure/return station usage counts."""

from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from helsinki_pipeline.etl.column_mapping import (
    DEPARTURE_STATION_ALIASES,
    RETURN_STATION_ALIASES,
    resolve_column,
)


def run_station_metrics_job(input_csv: Path, output_csv: Path) -> Path:
    """Compute station usage counts with Spark and write one CSV output file."""

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("helsinki_station_metrics")
        .getOrCreate()
    )

    try:
        frame = spark.read.option("header", True).csv(str(input_csv))
        dep_col = resolve_column(frame.columns, DEPARTURE_STATION_ALIASES)
        ret_col = resolve_column(frame.columns, RETURN_STATION_ALIASES)

        departures = (
            frame.groupBy(F.col(dep_col).alias("station_name"))
            .count()
            .withColumnRenamed("count", "ride_count")
            .withColumn("metric_type", F.lit("departure_name"))
        )

        returns = (
            frame.groupBy(F.col(ret_col).alias("station_name"))
            .count()
            .withColumnRenamed("count", "ride_count")
            .withColumn("metric_type", F.lit("return_name"))
        )

        combined = departures.unionByName(returns).select(
            "metric_type",
            "station_name",
            "ride_count",
        )

        temp_dir = output_csv.parent / f"{output_csv.stem}_spark_dir"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)

        combined.coalesce(1).write.mode("overwrite").option("header", True).csv(str(temp_dir))

        part_files = list(temp_dir.glob("part-*.csv"))
        if not part_files:
            raise FileNotFoundError("Spark output did not contain a part CSV file.")

        output_csv.parent.mkdir(parents=True, exist_ok=True)
        if output_csv.exists():
            output_csv.unlink()

        shutil.move(str(part_files[0]), str(output_csv))
        return output_csv
    finally:
        spark.stop()
