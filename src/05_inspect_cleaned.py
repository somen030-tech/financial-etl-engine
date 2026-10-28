
from pathlib import Path
import json

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
REPORT_DIR = ROOT / "data" / "reports"

reports = list(REPORT_DIR.glob("cleaning_*.json"))

if not reports:
    raise FileNotFoundError(
        "Run src/04_clean_data.py first."
    )

report_path = max(
    reports,
    key=lambda path: path.stat().st_mtime,
)

report = json.loads(
    report_path.read_text(encoding="utf-8")
)

output_dir = Path(report["output_directory"])
files = sorted(output_dir.glob("*.parquet"))

if not files:
    raise FileNotFoundError(
        f"No cleaned Parquet files in {output_dir}"
    )

df = pd.read_parquet(files[0])

print("\nCleaned data sample:")
print(df.head())

print("\nData types:")
print(df.dtypes.head(15))

print("\nMissing-value summary:")
for column, metrics in sorted(
    report["missingness"].items(),
    key=lambda item: item[1]["missing_percentage"],
    reverse=True,
)[:10]:
    print(
        f'{column}: {metrics["missing_count"]} missing '
        f'({metrics["missing_percentage"]:.2f}%)'
    )

print("\nRows cleaned:", report["rows_cleaned"])
print("Output directory:", output_dir)
