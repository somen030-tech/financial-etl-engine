
import json
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]

QUALITY_ROOT = ROOT / "data" / "incremental" / "quality"
DB_DIR = ROOT / "data" / "models"
DB_PATH = DB_DIR / "financial_analytics.duckdb"


def find_validated_batches():
    """Return all successful validation reports, oldest first."""
    batches = []

    if not QUALITY_ROOT.exists():
        raise FileNotFoundError(
            f"Quality directory not found: {QUALITY_ROOT}"
        )

    for report_path in QUALITY_ROOT.glob("*/report.json"):
        try:
            report = json.loads(
                report_path.read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError):
            continue

        accepted_dir = report_path.parent / "accepted"
        if report.get("status") != "SUCCESS" or not accepted_dir.is_dir():
            continue

        batch_hash = report.get("batch_hash")
        if not batch_hash:
            continue

        batches.append({
            "hash": batch_hash,
            "report": report,
            "report_path": report_path,
            "accepted_dir": accepted_dir,
        })

    batches.sort(
        key=lambda item: (
            item["report"].get("completed_at_utc", ""),
            item["hash"],
        )
    )

    return batches


def create_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS etl_batch_ledger (
            batch_hash VARCHAR PRIMARY KEY,
            source_row_count BIGINT NOT NULL,
            loaded_row_count BIGINT NOT NULL,
            status VARCHAR NOT NULL,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_transactions_incremental (
            transaction_id BIGINT PRIMARY KEY,
            transaction_dt BIGINT,
            transaction_amount DOUBLE,
            is_fraud INTEGER,
            product_code VARCHAR,
            card_brand VARCHAR,
            card_type VARCHAR,
            purchaser_email_domain VARCHAR,
            recipient_email_domain VARCHAR,
            relative_day_bucket BIGINT,
            source_batch_hash VARCHAR NOT NULL
        )
    """)


def load_one_batch(con, batch):
    batch_hash = batch["hash"]
    report = batch["report"]

    files = sorted(batch["accepted_dir"].glob("part-*.parquet"))

    if not files:
        raise RuntimeError(
            f"Batch {batch_hash} has no accepted Parquet files."
        )

    file_paths = [str(path) for path in files]

    # Check whether this batch was already committed.
    already_loaded = con.execute(
        """
        SELECT status, loaded_row_count
        FROM etl_batch_ledger
        WHERE batch_hash = ?
        """,
        [batch_hash],
    ).fetchone()

    if already_loaded and already_loaded[0] == "SUCCESS":
        print(
            f"SKIPPED {batch_hash}: already loaded "
            f"({already_loaded[1]:,} rows)"
        )
        return

    expected_rows = int(report["accepted_rows"])

    try:
        con.execute("BEGIN TRANSACTION")

        con.execute("DROP TABLE IF EXISTS stage_batch")

        con.execute(
            """
            CREATE TEMP TABLE stage_batch AS
            SELECT
                CAST(TransactionID AS BIGINT) AS transaction_id,
                CAST(TransactionDT AS BIGINT) AS transaction_dt,
                CAST(TransactionAmt AS DOUBLE) AS transaction_amount,
                CAST(isFraud AS INTEGER) AS is_fraud,
                CAST(ProductCD AS VARCHAR) AS product_code,
                CAST(card4 AS VARCHAR) AS card_brand,
                CAST(card6 AS VARCHAR) AS card_type,
                CAST(P_emaildomain AS VARCHAR) AS purchaser_email_domain,
                CAST(R_emaildomain AS VARCHAR) AS recipient_email_domain,
                CAST(FLOOR(TransactionDT / 86400.0) AS BIGINT)
                    AS relative_day_bucket
            FROM read_parquet(?)
            """,
            [file_paths],
        )

        staged_rows = con.execute(
            "SELECT COUNT(*) FROM stage_batch"
        ).fetchone()[0]

        if staged_rows != expected_rows:
            raise RuntimeError(
                f"Batch row-count mismatch: report says "
                f"{expected_rows}, Parquet contains {staged_rows}."
            )

        # Check duplicate IDs within this incoming batch.
        internal_duplicates = con.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT transaction_id
                FROM stage_batch
                GROUP BY transaction_id
                HAVING COUNT(*) > 1
            ) duplicates
        """).fetchone()[0]

        if internal_duplicates:
            raise RuntimeError(
                f"Batch {batch_hash} contains "
                f"{internal_duplicates} duplicated transaction IDs."
            )

        # Check collisions against previously loaded batches.
        existing_collisions = con.execute("""
            SELECT COUNT(*)
            FROM stage_batch AS incoming
            INNER JOIN fact_transactions_incremental AS existing
                USING (transaction_id)
        """).fetchone()[0]

        if existing_collisions:
            raise RuntimeError(
                f"Batch {batch_hash} contains "
                f"{existing_collisions} transaction IDs already loaded. "
                "The batch was not committed. Investigate the overlap."
            )

        # Insert transaction records and ledger entry atomically.
        con.execute(
            """
            INSERT INTO fact_transactions_incremental
            SELECT
                transaction_id,
                transaction_dt,
                transaction_amount,
                is_fraud,
                product_code,
                card_brand,
                card_type,
                purchaser_email_domain,
                recipient_email_domain,
                relative_day_bucket,
                ?
            FROM stage_batch
            """,
            [batch_hash],
        )

        con.execute(
            """
            INSERT INTO etl_batch_ledger (
                batch_hash,
                source_row_count,
                loaded_row_count,
                status
            )
            VALUES (?, ?, ?, 'SUCCESS')
            """,
            [batch_hash, expected_rows, staged_rows],
        )

        con.execute("COMMIT")

        print(
            f"LOADED {batch_hash}: "
            f"{staged_rows:,} rows committed"
        )

    except Exception:
        con.execute("ROLLBACK")
        raise

    finally:
        con.execute("DROP TABLE IF EXISTS stage_batch")


def main():
    DB_DIR.mkdir(parents=True, exist_ok=True)

    batches = find_validated_batches()

    if not batches:
        raise RuntimeError(
            "No validated batches found. Run "
            "src\\10_validate_incremental.py first."
        )

    con = duckdb.connect(str(DB_PATH))

    try:
        create_tables(con)

        for batch in batches:
            load_one_batch(con, batch)

        print("\nBATCH LOAD LEDGER")
        print("-----------------")
        print(
            con.execute("""
                SELECT
                    batch_hash,
                    source_row_count,
                    loaded_row_count,
                    status,
                    loaded_at
                FROM etl_batch_ledger
                ORDER BY loaded_at
            """).df().to_string(index=False)
        )

        print("\nFACT TABLE SUMMARY")
        print("------------------")
        print(
            con.execute("""
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(DISTINCT transaction_id) AS distinct_ids,
                    COUNT(DISTINCT source_batch_hash) AS loaded_batches
                FROM fact_transactions_incremental
            """).df().to_string(index=False)
        )

    finally:
        con.close()


if __name__ == "__main__":
    main()
