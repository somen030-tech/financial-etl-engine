
from pathlib import Path
from datetime import datetime, timezone
import json
import logging
import uuid

import pandas as pd


# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent
REPORT_DIR = ROOT / "data" / "reports"
LOG_DIR = ROOT / "data" / "logs"

CATEGORICAL_COLUMNS = [
    "ProductCD",
    "card4",
    "card6",
    "P_emaildomain",
    "R_emaildomain",
]

NUMERIC_COLUMNS = [
    "TransactionID",
    "TransactionDT",
    "TransactionAmt",
    "isFraud",
]

for directory in [REPORT_DIR, LOG_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_DIR / "cleaning.log",
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# 2. Find the latest quality-validated dataset
# --------------------------------------------------

def get_latest_quality_run():
    reports = list(REPORT_DIR.glob("quality_*.json"))

    if not reports:
        raise FileNotFoundError(
            "No quality report found. Run src/02_quality.py first."
        )

    latest_report = max(
        reports,
        key=lambda path: path.stat().st_mtime,
    )

    report = json.loads(
        latest_report.read_text(encoding="utf-8")
    )

    if report.get("status") != "SUCCESS":
        raise ValueError(
            "The latest quality run did not succeed."
        )

    input_dir = Path(report["accepted_output"])

    if not input_dir.exists():
        raise FileNotFoundError(
            f"Accepted data directory not found: {input_dir}"
        )

    return latest_report, report, input_dir


# --------------------------------------------------
# 3. Clean and standardize one DataFrame
# --------------------------------------------------

def clean_dataframe(df):
    df = df.copy()

    # Convert expected numeric columns to numeric types.
    # Invalid conversions become null, and are counted.
    conversion_errors = {}

    for column in NUMERIC_COLUMNS:
        if column not in df.columns:
            continue

        original_non_null = df[column].notna()

        converted = pd.to_numeric(
            df[column],
            errors="coerce",
        )

        conversion_errors[column] = int(
            (original_non_null & converted.isna()).sum()
        )

        df[column] = converted

    # Use nullable integer types for identifiers, timestamps
    # and the binary target, preserving missing values.
    for column in [
        "TransactionID",
        "TransactionDT",
        "isFraud",
    ]:
        if column not in df.columns:
            continue

        values = df[column]

        if values.notna().all() and (
            values % 1 == 0
        ).all():
            df[column] = values.astype("int64")
        else:
            df[column] = values.astype("Int64")

    # Standardize categorical text without inventing values.
    # Missing categories remain missing.
    for column in CATEGORICAL_COLUMNS:
        if column not in df.columns:
            continue

        df[column] = (
            df[column]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
        )

    return df, conversion_errors


# --------------------------------------------------
# 4. Run the cleaning pipeline
# --------------------------------------------------

def main():
    quality_report_path, quality_report, input_dir = (
        get_latest_quality_run()
    )

    input_files = sorted(input_dir.glob("*.parquet"))

    if not input_files:
        raise FileNotFoundError(
            f"No accepted Parquet files in {input_dir}"
        )

    run_id = str(uuid.uuid4())

    output_dir = ROOT / "data" / "silver_clean" / run_id
    output_dir.mkdir(parents=True, exist_ok=False)

    total_rows = 0
    conversion_totals = {}
    missing_totals = {}
    column_names = set()

    logger.info("Starting cleaning run %s", run_id)
    logger.info("Input quality report: %s", quality_report_path)

    for file_number, source_file in enumerate(
        input_files,
        start=1,
    ):
        df = pd.read_parquet(source_file)

        cleaned, conversion_errors = clean_dataframe(df)

        total_rows += len(cleaned)
        column_names.update(cleaned.columns)

        for column, count in conversion_errors.items():
            conversion_totals[column] = (
                conversion_totals.get(column, 0) + count
            )

        # Aggregate null counts across batches.
        for column, count in cleaned.isna().sum().items():
            missing_totals[column] = (
                missing_totals.get(column, 0) + int(count)
            )

        output_file = (
            output_dir / f"cleaned_{file_number:04d}.parquet"
        )

        cleaned.to_parquet(
            output_file,
            index=False,
            compression="snappy",
        )

        logger.info(
            "Cleaned %s rows from %s",
            len(cleaned),
            source_file.name,
        )

    # Report missingness across the complete cleaned dataset.
    missingness = {}

    for column in sorted(column_names):
        missing_count = missing_totals.get(column, 0)

        missingness[column] = {
            "missing_count": missing_count,
            "missing_percentage": (
                round(100 * missing_count / total_rows, 4)
                if total_rows
                else 0.0
            ),
        }

    report = {
        "run_id": run_id,
        "status": "SUCCESS",
        "started_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_quality_run_id": quality_report["run_id"],
        "input_files": len(input_files),
        "rows_cleaned": total_rows,
        "columns": len(column_names),
        "conversion_errors": conversion_totals,
        "missingness": missingness,
        "output_directory": str(output_dir),
        "note": (
            "Missing values were profiled, not globally imputed. "
            "TransactionDT remains the source-relative time field."
        ),
    }

    report_path = REPORT_DIR / f"cleaning_{run_id}.json"

    report_path.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    logger.info("Cleaning completed successfully.")
    logger.info("Rows processed: %s", total_rows)
    logger.info("Output: %s", output_dir)
    logger.info("Report: %s", report_path)

    print("\nCLEANING SUMMARY")
    print("Rows cleaned:", total_rows)
    print("Columns:", len(column_names))
    print("Output directory:", output_dir)
    print("Report:", report_path)

    print("\nTop 15 columns by missing-value percentage:")

    top_missing = sorted(
        missingness.items(),
        key=lambda item: item[1]["missing_percentage"],
        reverse=True,
    )[:15]

    for column, metrics in top_missing:
        print(
            f"{column}: "
            f'{metrics["missing_percentage"]:.2f}% missing'
        )


if __name__ == "__main__":
    main()
