"""CLI entrypoint to split Helsinki rides CSV into monthly CSV files."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from helsinki_pipeline.etl.split_monthly import MonthlyCsvSplitter  # noqa: E402


def parse_args() -> argparse.Namespace:
    """Parse command line arguments for monthly split process."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-csv",
        required=True,
        help="Path to source Helsinki bikes CSV.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "data" / "monthly"),
        help="Directory where monthly CSV files will be written.",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=200_000,
        help="Rows per chunk while reading the source file.",
    )
    return parser.parse_args()


def main() -> None:
    """Execute split process from CLI arguments."""

    args = parse_args()
    splitter = MonthlyCsvSplitter(chunksize=args.chunksize)
    splitter.split_file(
        source_csv=Path(args.input_csv),
        output_dir=Path(args.output_dir),
    )
    print(f"Monthly files created in: {args.output_dir}")


if __name__ == "__main__":
    main()

