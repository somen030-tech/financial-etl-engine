
from pathlib import Path
from datetime import datetime, timezone
import json
import logging
import uuid

import pandas as pd


# --------------------------------------------------
# 1. Project paths and configuration
# --------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parent.parent

SOURCE_FILE = (
    PROJECT_DIR / "data" / "raw" / "train_transaction.csv"
)

OUTPUT_DIR = PROJECT_DIR / "data" / "bronze" / "transactions"
LOG_DIR = PROJECT_DIR / "data" / "logs"

CHUNK_SIZE = 50_000

REQUIRED_COLUMNS = {
    "TransactionID",
    "TransactionDT",
    "TransactionAmt",
    "ProductCD",
    "isFraud",
}


# --------------------------------------------------
# 2. Configure logging
# --------------------------------------------------

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_DIR / "ingestion.log",
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# 3. Ingestion pipeline
# --------------------------------------------------

def ingest_transactions():

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Dataset not found: {SOURCE_FILE}\n"
            "Download train_transaction.csv from Kaggle "
            "and place it in data/raw/."
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    batch_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    logger.info("Starting ingestion batch: %s", batch_id)

    # Validate the source header before processing data.
    header = pd.read_csv(SOURCE_FILE, nrows=0)
    actual_columns = set(header.columns)

    missing_columns = REQUIRED_COLUMNS - actual_columns

    if missing_columns:
        raise ValueError(
            f"Required columns missing: {sorted(missing_columns)}"
        )

    # Record unexpected schema changes for investigation.
    unexpected_columns = sorted(
        actual_columns - REQUIRED_COLUMNS
    )

    logger.info("Source schema validated.")
    logger.info("Source columns: %s", len(actual_columns))

    if unexpected_columns:
        logger.info(
            "Additional source columns detected: %s",
            len(unexpected_columns),
        )

    # Track duplicate transaction IDs across all chunks.
    # The set is manageable for this dataset's row count.
    seen_ids = set()

    total_rows = 0
    total_duplicates = 0
    total_null_ids = 0
    total_invalid_amounts = 0
    chunk_count = 0

    try:
        for chunk_number, chunk in enumerate(
            pd.read_csv(
                SOURCE_FILE,
                chunksize=CHUNK_SIZE,
                low_memory=True,
            ),
            start=1,
        ):
            logger.info(
                "Processing chunk %s with %s rows",
                chunk_number,
                len(chunk),
            )

            # Check required fields.
            null_ids = int(
                chunk["TransactionID"].isna().sum()
            )
            total_null_ids += null_ids

            # Detect invalid amounts without silently
            # modifying the original transaction values.
            amounts = pd.to_numeric(
                chunk["TransactionAmt"],
                errors="coerce",
            )

            invalid_amounts = (
                amounts.isna()
                | (amounts < 0)
            )

            total_invalid_amounts += int(
                invalid_amounts.sum()
            )

            # Detect duplicate IDs within and across chunks.
            duplicate_mask = chunk[
                "TransactionID"
            ].duplicated(keep="first")

            duplicate_mask |= (
                chunk["TransactionID"].isin(seen_ids)
            )

            duplicate_mask &= (
                chunk["TransactionID"].notna()
            )

            total_duplicates += int(
                duplicate_mask.sum()
            )

            seen_ids.update(
                chunk.loc[
                    chunk["TransactionID"].notna(),
                    "TransactionID",
                ].tolist()
            )

            # Preserve raw rows in the bronze layer.
            # Quality failures are recorded for later handling.
            output_file = (
                OUTPUT_DIR
                / f"batch_{batch_id}_part_{chunk_number:04d}.parquet"
            )

            chunk.to_parquet(
                output_file,
                index=False,
                compression="snappy",
            )

            total_rows += len(chunk)
            chunk_count += 1

            logger.info(
                "Saved %s rows to %s",
                len(chunk),
                output_file.name,
            )

    except Exception:
        logger.exception(
            "Ingestion failed for batch %s", batch_id
        )
        raise

    finished_at = datetime.now(timezone.utc)

    # Create a batch manifest for auditing.
    manifest = {
        "batch_id": batch_id,
        "status": "SUCCESS",
        "source_file": SOURCE_FILE.name,
        "source_columns": sorted(actual_columns),
        "unexpected_columns": unexpected_columns,
        "rows_ingested": total_rows,
        "chunks_written": chunk_count,
        "duplicate_transaction_ids": total_duplicates,
        "null_transaction_ids": total_null_ids,
        "invalid_transaction_amounts": total_invalid_amounts,
        "started_at_utc": started_at.isoformat(),
        "finished_at_utc": finished_at.isoformat(),
    }

    manifest_path = (
        LOG_DIR / f"manifest_{batch_id}.json"
    )

    manifest_path.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    logger.info("Ingestion completed.")
    logger.info("Rows ingested: %s", total_rows)
    logger.info("Duplicate IDs: %s", total_duplicates)
    logger.info("Manifest saved: %s", manifest_path)


if __name__ == "__main__":
    ingest_transactions()
