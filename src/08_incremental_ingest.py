
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import logging
import shutil
import sqlite3
import uuid

import pandas as pd


# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent

SOURCE_FILE = ROOT / "data" / "raw" / "train_transaction.csv"

BASE_DIR = ROOT / "data" / "incremental"
BRONZE_DIR = BASE_DIR / "bronze"
STAGING_DIR = BASE_DIR / "staging"

REGISTRY_DB = BASE_DIR / "batch_registry.sqlite"
LOG_DIR = ROOT / "data" / "logs"

CHUNK_SIZE = 50_000

REQUIRED_COLUMNS = {
    "TransactionID",
    "TransactionDT",
    "TransactionAmt",
    "ProductCD",
    "isFraud",
}

for folder in [BRONZE_DIR, STAGING_DIR, LOG_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_DIR / "incremental_ingestion.log",
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# 2. Initialize the batch registry
# --------------------------------------------------

def initialize_registry():
    with sqlite3.connect(REGISTRY_DB) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_batches (
                file_hash TEXT PRIMARY KEY,
                source_name TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                chunk_count INTEGER NOT NULL,
                output_path TEXT NOT NULL,
                status TEXT NOT NULL,
                processed_at_utc TEXT NOT NULL
            )
            """
        )


# --------------------------------------------------
# 3. Compute a fingerprint without loading the
#    entire CSV into memory
# --------------------------------------------------

def calculate_sha256(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file:
        while True:
            block = file.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


# --------------------------------------------------
# 4. Registry operations
# --------------------------------------------------

def get_registered_batch(file_hash):
    with sqlite3.connect(REGISTRY_DB) as con:
        con.row_factory = sqlite3.Row

        row = con.execute(
            """
            SELECT *
            FROM processed_batches
            WHERE file_hash = ?
              AND status = 'SUCCESS'
            """,
            (file_hash,),
        ).fetchone()

        return dict(row) if row else None


def register_success(file_hash, manifest):
    with sqlite3.connect(REGISTRY_DB) as con:
        con.execute(
            """
            INSERT INTO processed_batches (
                file_hash,
                source_name,
                batch_id,
                row_count,
                chunk_count,
                output_path,
                status,
                processed_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, 'SUCCESS', ?)
            ON CONFLICT(file_hash) DO UPDATE SET
                source_name = excluded.source_name,
                batch_id = excluded.batch_id,
                row_count = excluded.row_count,
                chunk_count = excluded.chunk_count,
                output_path = excluded.output_path,
                status = excluded.status,
                processed_at_utc = excluded.processed_at_utc
            """,
            (
                file_hash,
                manifest["source_name"],
                manifest["batch_id"],
                manifest["row_count"],
                manifest["chunk_count"],
                manifest["output_path"],
                manifest["processed_at_utc"],
            ),
        )


# --------------------------------------------------
# 5. Ingest one source file
# --------------------------------------------------

def ingest_file():
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Source CSV not found: {SOURCE_FILE}"
        )

    initialize_registry()

    logger.info("Calculating source fingerprint...")
    file_hash = calculate_sha256(SOURCE_FILE)

    logger.info("SHA-256: %s", file_hash)

    existing = get_registered_batch(file_hash)

    if existing:
        output_path = Path(existing["output_path"])

        if not output_path.exists():
            raise RuntimeError(
                "Registry says this batch was processed, "
                "but its output directory is missing. "
                "Investigate before retrying."
            )

        logger.info(
            "Already processed successfully. Skipping batch %s",
            existing["batch_id"],
        )

        print("\nIDEMPOTENCY CHECK PASSED")
        print("Status: SKIPPED")
        print("Reason: identical file already processed")
        print("Batch ID:", existing["batch_id"])
        print("Rows previously ingested:", existing["row_count"])
        return

    # Deterministic output location for this exact file content.
    output_dir = BRONZE_DIR / file_hash
    final_manifest_path = output_dir / "manifest.json"

    # Recover if publication succeeded but the registry write
    # was interrupted. Do not trust a directory without a manifest.
    if output_dir.exists() and final_manifest_path.exists():
        manifest = json.loads(
            final_manifest_path.read_text(encoding="utf-8")
        )

        if manifest.get("file_hash") != file_hash:
            raise RuntimeError("Existing manifest hash mismatch.")

        if manifest.get("status") != "SUCCESS":
            raise RuntimeError("Existing batch is not marked successful.")

        if manifest.get("source_name") != SOURCE_FILE.name:
            raise RuntimeError("Existing manifest source mismatch.")

        if not list(output_dir.glob("part-*.parquet")):
            raise RuntimeError("Published batch has no Parquet parts.")

        register_success(file_hash, manifest)

        logger.info("Recovered published batch in registry.")

        print("\nRECOVERED EXISTING BATCH")
        print("Batch ID:", manifest["batch_id"])
        print("Rows:", manifest["row_count"])
        return

    # An incomplete output without a manifest is not published.
    # Remove it before restarting the same content-addressed batch.
    if output_dir.exists():
        logger.warning("Removing incomplete unpublished output.")
        shutil.rmtree(output_dir)

    batch_id = str(uuid.uuid4())
    staging_dir = STAGING_DIR / batch_id
    staging_dir.mkdir(parents=True)

    started_at = datetime.now(timezone.utc).isoformat()
    total_rows = 0
    chunk_count = 0
    published = False

    try:
        # Validate the CSV header before reading all rows.
        header = pd.read_csv(SOURCE_FILE, nrows=0)

        missing = REQUIRED_COLUMNS - set(header.columns)

        if missing:
            raise ValueError(
                f"Missing required columns: {sorted(missing)}"
            )

        for chunk_number, chunk in enumerate(
            pd.read_csv(
                SOURCE_FILE,
                chunksize=CHUNK_SIZE,
                low_memory=True,
            ),
            start=1,
        ):
            part_path = (
                staging_dir / f"part-{chunk_number:05d}.parquet"
            )

            chunk.to_parquet(
                part_path,
                index=False,
                compression="snappy",
            )

            total_rows += len(chunk)
            chunk_count += 1

            logger.info(
                "Wrote chunk %s: %s rows",
                chunk_number,
                len(chunk),
            )

        # A source with no data rows is not accepted as a valid batch.
        if total_rows == 0:
            raise ValueError("Source CSV contains no data rows.")

        manifest = {
            "batch_id": batch_id,
            "file_hash": file_hash,
            "source_name": SOURCE_FILE.name,
            "row_count": total_rows,
            "chunk_count": chunk_count,
            "output_path": str(output_dir),
            "status": "SUCCESS",
            "started_at_utc": started_at,
            "processed_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
        }

        (staging_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2),
            encoding="utf-8",
        )

        # Publish only after every chunk and manifest are complete.
        # Both paths are under the same parent filesystem.
        staging_dir.rename(output_dir)
        published = True

        # If this fails, the next run can recover from the manifest.
        register_success(file_hash, manifest)

        logger.info("Incremental ingestion completed.")
        logger.info("Rows ingested: %s", total_rows)
        logger.info("Output: %s", output_dir)

        print("\nINGESTION SUCCESS")
        print("Batch ID:", batch_id)
        print("Rows ingested:", total_rows)
        print("Chunks written:", chunk_count)
        print("Output:", output_dir)

    except Exception:
        logger.exception("Incremental ingestion failed.")

        # Never leave a partial batch at the published path.
        # If publication already occurred, keep the manifest
        # so a later run can attempt registry recovery.
        if not published and staging_dir.exists():
            shutil.rmtree(staging_dir)

        raise


if __name__ == "__main__":
    ingest_file()
