
import json
from pathlib import Path

import pandas as pd

from quality_rules import inspect_schema


ROOT = Path(__file__).resolve().parents[1]

SOURCE_FILE = ROOT / "data" / "raw" / "train_transaction.csv"
REPORT_DIR = ROOT / "data" / "reports"
BASELINE_FILE = REPORT_DIR / "schema_baseline.json"


def main():
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Dataset not found: {SOURCE_FILE}"
        )

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Read only the CSV header; do not load the large dataset.
    columns = pd.read_csv(SOURCE_FILE, nrows=0).columns.tolist()

    result = inspect_schema(columns, BASELINE_FILE)
    result["source_file"] = str(SOURCE_FILE)
    result["column_count"] = len(columns)

    report_file = REPORT_DIR / "schema_check_report.json"
    report_file.write_text(
        json.dumps(result, indent=2),
        encoding="utf-8",
    )

    print("Schema check completed.")
    print(f"Status: {result['status']}")
    print(f"Current column count: {len(columns)}")
    print(f"Report: {report_file}")

    if result["missing_columns"]:
        print("Missing columns:", result["missing_columns"])

    if result["unexpected_columns"]:
        print("Unexpected columns:", result["unexpected_columns"])

    if result["missing_required_columns"]:
        print(
            "Missing required columns:",
            result["missing_required_columns"],
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
