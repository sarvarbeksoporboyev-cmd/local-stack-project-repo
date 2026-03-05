"""Export DynamoDB period metrics to CSV files for Tableau Public dashboards."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from helsinki_pipeline.aws.aws_clients import create_resource  # noqa: E402
from helsinki_pipeline.config import AppConfig  # noqa: E402


def export_metrics_for_tableau(use_docker_endpoint: bool = False) -> None:
    """Scan metrics table and export daily/monthly CSVs for Tableau use."""

    cfg = AppConfig.from_env(PROJECT_ROOT / ".env", use_docker_endpoint=use_docker_endpoint)
    dynamodb = create_resource("dynamodb", cfg)
    table = dynamodb.Table(cfg.dynamodb_period_metrics_table)

    all_items: list[dict] = []
    response = table.scan()
    all_items.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        all_items.extend(response.get("Items", []))

    if not all_items:
        print("No metric rows found in DynamoDB table.")
        return

    export_dir = PROJECT_ROOT / "tableau_exports"
    export_dir.mkdir(parents=True, exist_ok=True)

    frame = pd.DataFrame(all_items)
    day_frame = frame.loc[frame["period_type"] == "day"].copy()
    month_frame = frame.loc[frame["period_type"] == "month"].copy()

    day_frame.to_csv(export_dir / "daily_metrics.csv", index=False)
    month_frame.to_csv(export_dir / "monthly_metrics.csv", index=False)

    print(f"Exported {len(day_frame)} daily rows and {len(month_frame)} monthly rows.")


if __name__ == "__main__":
    export_metrics_for_tableau(use_docker_endpoint=False)

