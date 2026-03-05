"""Split a large Helsinki rides CSV into one file per month."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from helsinki_pipeline.etl.column_mapping import DEPARTURE_TIME_ALIASES, resolve_column


@dataclass
class MonthlyCsvSplitter:
    """Chunk-based monthly splitter for large bike ride CSV datasets."""

    chunksize: int = 200_000

    def split_file(self, source_csv: Path, output_dir: Path) -> None:
        """Split one source CSV by departure month and write monthly CSV files."""

        output_dir.mkdir(parents=True, exist_ok=True)
        wrote_header: dict[Path, bool] = {}

        for chunk in pd.read_csv(source_csv, chunksize=self.chunksize, low_memory=False):
            departure_col = resolve_column(chunk.columns, DEPARTURE_TIME_ALIASES)
            chunk[departure_col] = pd.to_datetime(chunk[departure_col], errors="coerce")
            chunk = chunk.dropna(subset=[departure_col])

            chunk["year_month"] = chunk[departure_col].dt.strftime("%Y-%m")
            for year_month, monthly_df in chunk.groupby("year_month"):
                file_path = output_dir / f"{year_month}.csv"
                write_header = not wrote_header.get(file_path, False)
                monthly_df.drop(columns=["year_month"]).to_csv(
                    file_path,
                    mode="a",
                    index=False,
                    header=write_header,
                )
                wrote_header[file_path] = True
