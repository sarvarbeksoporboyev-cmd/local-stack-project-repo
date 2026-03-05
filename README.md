# Helsinki City Bikes Local Cloud Stack

Local implementation of an AWS-style data platform using **LocalStack + Airflow + Spark + Lambda + DynamoDB**.

## What This Project Covers

1. Split Helsinki city bikes source data into monthly CSV files.
2. Airflow DAG to ingest all CSV files from a folder and upload to S3.
3. Airflow DAG to ingest one CSV and run Spark metrics:
   - rides grouped by `departure_name`
   - rides grouped by `return_name`
4. S3 trigger -> SNS -> SQS -> Lambda pipeline.
5. Lambda waits for both files (raw + Spark metrics) before processing.
6. DynamoDB tables for raw trips and period metrics (day, month).
7. Lambda uses Pandas to compute and persist metrics:
   - AVG Distance day
   - AVG Duration day
   - AVG Speed day
   - AVG Temperature
8. Tableau-ready CSV export from DynamoDB metrics.

## Architecture

- `data/raw/*.csv`: source dataset from Kaggle.
- `scripts/run_splitter.py`: splits source into `data/monthly/YYYY-MM.csv`.
- `airflow`:
  - `bulk_upload_dropzone_to_s3` DAG uploads all `data/input_dropzone/*.csv`.
  - `single_file_to_s3_with_spark_metrics` DAG uploads one file + Spark metrics file.
- `s3:ObjectCreated` notifications publish to SNS topic.
- SNS forwards to SQS queue.
- Lambda is triggered by SQS, waits until both file types exist, then writes:
  - raw rows -> `bike_trips_raw`
  - day/month aggregates -> `bike_period_metrics`
  - pair state -> `bike_file_state`

## Repository Structure

- `airflow/`: custom Airflow image and DAGs.
- `lambdas/dynamodb_ingestor/`: Lambda source and requirements.
- `scripts/bootstrap_localstack.py`: creates LocalStack resources.
- `scripts/package_lambda.py`: packages Lambda with Pandas.
- `scripts/export_tableau_data.py`: exports DynamoDB metrics for Tableau.
- `src/helsinki_pipeline/`: modular application code (PEP8, docstrings, SOLID-oriented structure).
- `tests/`: unit tests for core logic.

## Prerequisites

- Docker Desktop + Docker Compose
- Python 3.10+
- Optional: AWS CLI + `awslocal`

## Setup

1. Put the Kaggle CSV in `data/raw` (for example: `data/raw/helsinki_city_bikes.csv`).
2. Split monthly files:

```bash
python scripts/run_splitter.py --input-csv data/raw/helsinki_city_bikes.csv --output-dir data/monthly
```

3. Copy a few monthly files to ingestion dropzone (manual test set):

```bash
copy data\monthly\2019-06.csv data\input_dropzone\2019-06.csv
copy data\monthly\2019-07.csv data\input_dropzone\2019-07.csv
```

4. Start LocalStack + Airflow:

```bash
docker compose up --build -d
```

5. Package Lambda with dependencies (Pandas included):

```bash
docker compose exec airflow python /opt/project/scripts/package_lambda.py
```

6. Bootstrap AWS resources in LocalStack:

```bash
docker compose exec airflow python /opt/project/scripts/bootstrap_localstack.py --docker-endpoint
```

7. Open Airflow UI: `http://localhost:8080` (`admin` / `admin`).

## Airflow Execution

### Bulk upload DAG

- DAG: `bulk_upload_dropzone_to_s3`
- Action: uploads all CSV files from `data/input_dropzone` to S3 raw prefix.

### Single file + Spark metrics DAG

- DAG: `single_file_to_s3_with_spark_metrics`
- Param: `input_file` (for example `2019-06.csv`)
- Action:
  - uploads raw file to S3
  - computes Spark metrics file
  - uploads metrics file to S3

When raw and metrics files are both present, Lambda processes them automatically.

## DynamoDB Tables

1. `bike_trips_raw`
   - PK: `trip_id`
   - stores normalized raw trip rows.

2. `bike_period_metrics`
   - PK: `period_type` (`day` or `month`)
   - SK: `period_value` (`YYYY-MM-DD` or `YYYY-MM`)
   - stores AVG distance, duration, speed, temperature.

3. `bike_file_state`
   - PK: `file_id`
   - tracks whether both raw and metrics files arrived and were processed.

## Tableau Public Steps

1. Export metrics from DynamoDB:

```bash
python scripts/export_tableau_data.py
```

2. Open Tableau Public and connect:
   - `tableau_exports/daily_metrics.csv`
   - `tableau_exports/monthly_metrics.csv`

3. Build dashboards by day and month (line charts, bars, filters).

## Code Quality Notes

- Modular services and single-responsibility classes.
- Naming aligned with domain terms.
- Configuration and credentials are externalized in `.env`.
- PEP8 formatting and docstrings/comments included.
- Unit tests provided for core transformations and S3 naming logic.

## Useful Commands

```bash
python -m pytest -q
python -m ruff check .
```
