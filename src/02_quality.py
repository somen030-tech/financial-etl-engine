
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

BRONZE_DIR = ROOT / "data" / "bronze" / "transactions"
SILVER_DIR = ROOT / "data" / "silver"
QUARANTINE_DIR = ROOT / "data" / "quarantine"
REPORT_DIR = ROOT / "data" / "reports"
LOG_DIR = ROOT / "data" / "logs"

REQUIRED_COLUMNS = {
    "TransactionID",
    "TransactionDT",
    "TransactionAmt",
    "isFraud",
}

VALID_FRAUD_LABELS = {0, 1}

for directory in [
    SILVER_DIR,
    QUARANTINE_DIR,
    REPORT_DIR,
    LOG_DIR,
]:
    directory.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_DIR / "quality.log",
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# 2. Validate one batch
# --------------------------------------------------

def validate_batch(
    df: pd.DataFrame,
    seen_ids: set,
):
    """
    Return accepted rows, rejected rows and quality metrics.

    The first occurrence of a non-null TransactionID is
    retained. Later occurrences are quarantined.
    """

    df = df.copy()

    required_missing = REQUIRED_COLUMNS - set(df.columns)

    if required_missing:
        raise ValueError(
            f"Required columns missing: {sorted(required_missing)}"
        )

    # Collect all applicable failure reasons per row.
    errors = [[] for _ in range(len(df))]

    def add_error(mask, message):
        for position in range(len(df)):
            if bool(mask.iloc[position]):
                errors[position].append(message)

    # Check required values.
    null_id = df["TransactionID"].isna()
    add_error(null_id, "NULL_TRANSACTION_ID")

    transaction_id_numeric = pd.to_numeric(
        df["TransactionID"],
        errors="coerce",
    )

    invalid_id = (
        transaction_id_numeric.isna()
        & ~null_id
    )
    add_error(invalid_id, "INVALID_TRANSACTION_ID")

    # Validate transaction timestamps.
    transaction_dt = pd.to_numeric(
        df["TransactionDT"],
        errors="coerce",
    )

    invalid_dt = (
        transaction_dt.isna()
        | ~transaction_dt.map(
            lambda x: pd.notna(x) and float(x).is_integer()
        )
        | (transaction_dt < 0)
    )
    add_error(invalid_dt, "INVALID_TRANSACTION_DT")

    # Validate amounts without silently changing source values.
    amount = pd.to_numeric(
        df["TransactionAmt"],
        errors="coerce",
    )

    invalid_amount = (
        amount.isna()
        | (amount < 0)
        | ~amount.map(
            lambda x: pd.notna(x) and abs(float(x)) != float("inf")
        )
    )
    add_error(invalid_amount, "INVALID_TRANSACTION_AMOUNT")

    # The training dataset's fraud target must be 0 or 1.
    fraud = pd.to_numeric(
        df["isFraud"],
        errors="coerce",
    )

    invalid_fraud = ~fraud.isin(VALID_FRAUD_LABELS)
    add_error(invalid_fraud, "INVALID_FRAUD_LABEL")

    # Detect duplicates within this batch and across earlier batches.
    duplicate_mask = df["TransactionID"].duplicated(
        keep="first"
    )

    duplicate_mask |= df["TransactionID"].isin(seen_ids)
    duplicate_mask &= df["TransactionID"].notna()

    add_error(duplicate_mask, "DUPLICATE_TRANSACTION_ID")

    # Track IDs after checking the whole batch.
    # Invalid/null IDs are not added to the set.
    valid_ids = df.loc[
        ~null_id & ~invalid_id,
        "TransactionID",
    ]

    seen_ids.update(valid_ids.tolist())

    # A row with one or more failures is quarantined.
    rejected_mask = pd.Series(
        [bool(row_errors) for row_errors in errors],
        index=df.index,
    )

    accepted = df.loc[~rejected_mask].copy()
    rejected = df.loc[rejected_mask].copy()

    # Store every applicable reason for later investigation.
    rejected["_quality_errors"] = [
        "|".join(errors[position])
        for position in range(len(df))
        if errors[position]
    ]

    metrics = {
        "input_rows": int(len(df)),
        "accepted_rows": int(len(accepted)),
        "rejected_rows": int(len(rejected)),
        "null_transaction_ids": int(null_id.sum()),
        "invalid_transaction_ids": int(invalid_id.sum()),
        "invalid_transaction_dt": int(invalid_dt.sum()),
        "invalid_transaction_amounts": int(invalid_amount.sum()),
        "invalid_fraud_labels": int(invalid_fraud.sum()),
        "duplicate_transaction_ids": int(duplicate_mask.sum()),
    }

    return accepted, rejected, metrics


# --------------------------------------------------
# 3. Process all bronze files
# --------------------------------------------------

def run_quality_checks():

    source_files = sorted(BRONZE_DIR.glob("*.parquet"))

    if not source_files:
        raise FileNotFoundError(
            f"No bronze Parquet files found in {BRONZE_DIR}. "
            "Run src/01_ingest.py first."
        )

    # Each run has separate output folders so previous runs
    # are not accidentally mixed with the current run.
    run_id = str(uuid.uuid4())

    silver_run_dir = SILVER_DIR / run_id
    quarantine_run_dir = QUARANTINE_DIR / run_id

    silver_run_dir.mkdir(parents=True)
    quarantine_run_dir.mkdir(parents=True)

    started_at = datetime.now(timezone.utc)
    seen_ids = set()

    totals = {
        "input_rows": 0,
        "accepted_rows": 0,
        "rejected_rows": 0,
        "null_transaction_ids": 0,
        "invalid_transaction_ids": 0,
        "invalid_transaction_dt": 0,
        "invalid_transaction_amounts": 0,
        "invalid_fraud_labels": 0,
        "duplicate_transaction_ids": 0,
    }

    logger.info("Starting quality run %s", run_id)

    for file_number, source_file in enumerate(
        source_files,
        start=1,
    ):
        logger.info(
            "Validating %s (%s of %s)",
            source_file.name,
            file_number,
            len(source_files),
        )

        df = pd.read_parquet(source_file)

        accepted, rejected, metrics = validate_batch(
            df,
            seen_ids,
        )

        # Use a unique file per source batch and output type.
        output_name = f"{file_number:04d}.parquet"

        if not accepted.empty:
            accepted.to_parquet(
                silver_run_dir / output_name,
                index=False,
                compression="snappy",
            )

        if not rejected.empty:
            rejected.to_parquet(
                quarantine_run_dir / output_name,
                index=False,
                compression="snappy",
            )

        for key in totals:
            totals[key] += metrics[key]

        logger.info(
            "Accepted=%s | Rejected=%s",
            metrics["accepted_rows"],
            metrics["rejected_rows"],
        )

    finished_at = datetime.now(timezone.utc)

    report = {
        "run_id": run_id,
        "status": "SUCCESS",
        "started_at_utc": started_at.isoformat(),
        "finished_at_utc": finished_at.isoformat(),
        "source_files": len(source_files),
        **totals,
        "rejection_rate": (
            totals["rejected_rows"] / totals["input_rows"]
            if totals["input_rows"]
            else 0.0
        ),
        "accepted_output": str(silver_run_dir),
        "quarantine_output": str(quarantine_run_dir),
    }

    report_path = REPORT_DIR / f"quality_{run_id}.json"

    report_path.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    logger.info("Quality validation finished.")
    logger.info("Input rows: %s", totals["input_rows"])
    logger.info("Accepted rows: %s", totals["accepted_rows"])
    logger.info("Rejected rows: %s", totals["rejected_rows"])
    logger.info("Report: %s", report_path)

    print("\nQUALITY REPORT")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    run_quality_checks()
