
from pathlib import Path
import json

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent

report_dir = ROOT / "data" / "reports"
reports = sorted(report_dir.glob("quality_*.json"))

if not reports:
    raise FileNotFoundError(
        "Run src/02_quality.py first."
    )

# Read the most recently created report.
report_path = max(reports, key=lambda p: p.stat().st_mtime)
report = json.loads(report_path.read_text(encoding="utf-8"))

print("Run ID:", report["run_id"])
print("Input rows:", report["input_rows"])
print("Accepted:", report["accepted_rows"])
print("Rejected:", report["rejected_rows"])
print("Rejection rate:", f'{report["rejection_rate"]:.2%}')

quarantine_dir = Path(report["quarantine_output"])
quarantine_files = sorted(quarantine_dir.glob("*.parquet"))

if quarantine_files:
    rejected = pd.read_parquet(quarantine_files[0])

    print("\nRejected record examples:")

    columns = [
        c for c in [
            "TransactionID",
            "TransactionAmt",
            "isFraud",
            "_quality_errors",
        ]
        if c in rejected.columns
    ]

    print(rejected[columns].head(10))
else:
    print("\nNo records were rejected in this run.")
