"""Build and zip Lambda source with dependencies for LocalStack deployment."""

from __future__ import annotations

import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LAMBDA_DIR = PROJECT_ROOT / "lambdas" / "dynamodb_ingestor"
BUILD_DIR = PROJECT_ROOT / "build"
TEMP_PACKAGE_DIR = BUILD_DIR / "lambda_package"
ZIP_PATH = BUILD_DIR / "dynamodb_ingestor.zip"


def build_lambda_package() -> Path:
    """Install Lambda dependencies and package them with source code."""

    requirements_file = LAMBDA_DIR / "requirements.txt"
    lambda_source = LAMBDA_DIR / "lambda_function.py"

    if TEMP_PACKAGE_DIR.exists():
        shutil.rmtree(TEMP_PACKAGE_DIR)
    TEMP_PACKAGE_DIR.mkdir(parents=True, exist_ok=True)
    BUILD_DIR.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "-r",
            str(requirements_file),
            "-t",
            str(TEMP_PACKAGE_DIR),
        ],
        check=True,
    )

    shutil.copy2(lambda_source, TEMP_PACKAGE_DIR / "lambda_function.py")

    if ZIP_PATH.exists():
        ZIP_PATH.unlink()

    with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED) as archive:
        for file_path in TEMP_PACKAGE_DIR.rglob("*"):
            if file_path.is_file():
                archive.write(file_path, file_path.relative_to(TEMP_PACKAGE_DIR))

    return ZIP_PATH


if __name__ == "__main__":
    package = build_lambda_package()
    print(f"Lambda package created: {package}")
