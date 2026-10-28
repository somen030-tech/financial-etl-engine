
from pathlib import Path
import json

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent

parquet_dir = ROOT / "data" / "bronze" / "transactions"
manifest_dir = ROOT / "data" / "logs"

files = sorted(parquet_dir.glob("*.parquet"))
manifests = sorted(manifest_dir.glob("manifest_*.json"))

if not files:
    raise FileNotFoundError(
        "No Parquet files found. Run src/01_ingest.py first."
    )

if not manifests:
    raise FileNotFoundError("No ingestion manifest found.")

# Show a sample without loading the complete dataset.
sample = pd.read_parquet(files[0])

manifest = json.loads(
    manifests[-1].read_text(encoding="utf-8")
)

print("\nSample records:")
print(
    sample[
        [
            "TransactionID",
            "TransactionDT",
            "TransactionAmt",
            "ProductCD",
            "isFraud",
        ]
    ].head()
)

print("\nIngestion manifest:")
print(json.dumps(manifest, indent=2))

print("\nParquet files written:", len(files))
print("Sample rows:", len(sample))
