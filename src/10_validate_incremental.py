
import json
import shutil
import sqlite3
import uuid

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from quality_rules import validate_transactions


ROOT = Path(__file__).resolve().parents[1]

REGISTRY = ROOT / "data" / "incremental" / "batch_registry.sqlite"
OUTPUT_ROOT = ROOT / "data" / "incremental" / "quality"

OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)


def latest_successful_batch():
    """Find the most recently registered successful ingestion batch."""
    if not REGISTRY.exists():
        raise FileNotFoundError(
            f"Batch registry not found: {REGISTRY}\n"
            "Run src\\08_incremental_ingest.py first."
        )

    with sqlite3.connect(REGISTRY) as connection:
        row = connection.execute(
            """
            SELECT file_hash, output_path, status
            FROM processed_batches
            WHERE status = 'SUCCESS'
            ORDER BY processed_at_utc DESC
            LIMIT 1
            """
        ).fetchone()

    if row is None:
        raise RuntimeError(
            "No successful ingestion batch found in the registry."
        )

    return {
        "file_hash": row[0],
        "output_path": Path(row[1]),
        "status": row[2],
    }


def main():
    batch = latest_successful_batch()
    file_hash = batch["file_hash"]
    bronze_dir = batch["output_path"]

    if not bronze_dir.exists():
        raise FileNotFoundError(
            f"Bronze batch directory not found: {bronze_dir}"
        )

    parquet_files = sorted(bronze_dir.glob("part-*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(
            f"No part-*.parquet files found in {bronze_dir}"
        )

    final_dir = OUTPUT_ROOT / file_hash
    report_file = final_dir / "report.json"

    # Skip completed runs when their expected output is present.
    if (
        report_file.exists()
        and (final_dir / "accepted").is_dir()
        and (final_dir / "quarantine").is_dir()
    ):
        saved_report = json.loads(
            report_file.read_text(encoding="utf-8")
        )
        if saved_report.get("status") == "SUCCESS":
            print("Quality validation already completed.")
            print(f"Batch hash: {file_hash}")
            print(f"Report: {report_file}")
            print("Skipping to avoid duplicate processing.")
            return

    # A deterministic output path makes re-runs idempotent.
    # Build everything in a temporary directory before publishing.
    if final_dir.exists():
        shutil.rmtree(final_dir)

    staging_dir = OUTPUT_ROOT / f".staging_{uuid.uuid4().hex}"
    accepted_dir = staging_dir / "accepted"
    quarantine_dir = staging_dir / "quarantine"

    accepted_dir.mkdir(parents=True)
    quarantine_dir.mkdir(parents=True)

    seen_ids = set()
    totals = Counter()
    rejection_reasons = Counter()

    try:
        for part_number, parquet_file in enumerate(
            parquet_files, start=1
        ):
            print(
                f"Validating {part_number}/{len(parquet_files)}: "
                f"{parquet_file.name}"
            )

            # Each Parquet file is one ingestion chunk.
            df = pd.read_parquet(parquet_file)

            accepted, rejected, metrics = validate_transactions(
                df, seen_ids=seen_ids
            )

            accepted.to_parquet(
                accepted_dir / parquet_file.name,
                index=False,
            )
            rejected.to_parquet(
                quarantine_dir / parquet_file.name,
                index=False,
            )

            totals.update(metrics)

            if not rejected.empty:
                for error_text in rejected["_quality_errors"]:
                    for reason in error_text.split("|"):
                        if reason:
                            rejection_reasons[reason] += 1

            del df, accepted, rejected

        report = {
            "status": "SUCCESS",
            "batch_hash": file_hash,
            "bronze_directory": str(bronze_dir),
            "input_parquet_files": len(parquet_files),
            "input_rows": totals["input_rows"],
            "accepted_rows": totals["accepted_rows"],
            "rejected_rows": totals["rejected_rows"],
            "duplicate_ids_detected": totals[
                "duplicate_ids_detected"
            ],
            "rejection_reasons": dict(rejection_reasons),
            "completed_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
        }

        (staging_dir / "report.json").write_text(
            json.dumps(report, indent=2),
            encoding="utf-8",
        )

        # Publish the complete output directory.
        staging_dir.rename(final_dir)

    except Exception:
        if staging_dir.exists():
            shutil.rmtree(staging_dir)
        raise

    print("\nQUALITY VALIDATION SUMMARY")
    print("--------------------------")
    print(f"Batch hash:       {file_hash}")
    print(f"Input rows:       {report['input_rows']:,}")
    print(f"Accepted rows:    {report['accepted_rows']:,}")
    print(f"Rejected rows:    {report['rejected_rows']:,}")
    print(
        "Duplicate IDs:    "
        f"{report['duplicate_ids_detected']:,}"
    )
    print(f"Report:           {final_dir / 'report.json'}")
    print(f"Accepted files:   {final_dir / 'accepted'}")
    print(f"Quarantine files: {final_dir / 'quarantine'}")


if __name__ == "__main__":
    main()
