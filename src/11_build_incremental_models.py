
import json
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]

QUALITY_ROOT = ROOT / "data" / "incremental" / "quality"
DB_DIR = ROOT / "data" / "models"
REPORT_DIR = ROOT / "data" / "reports"

DB_PATH = DB_DIR / "financial_analytics.duckdb"


def find_latest_validated_batch():
    """Find the latest batch with a successful validation report."""
    candidates = []

    if not QUALITY_ROOT.exists():
        raise FileNotFoundError(
            f"Quality output not found: {QUALITY_ROOT}"
        )

    for report_path in QUALITY_ROOT.glob("*/report.json"):
        try:
            report = json.loads(
                report_path.read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError):
            continue

        if (
            report.get("status") == "SUCCESS"
            and (report_path.parent / "accepted").is_dir()
        ):
            candidates.append(
                (report.get("completed_at_utc", ""), report_path)
            )

    if not candidates:
        raise RuntimeError(
            "No successfully validated incremental batch found. "
            "Run src\\10_validate_incremental.py first."
        )

    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def main():
    DB_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    validation_report_path = find_latest_validated_batch()
    batch_dir = validation_report_path.parent
    accepted_dir = batch_dir / "accepted"

    validation_report = json.loads(
        validation_report_path.read_text(encoding="utf-8")
    )

    parquet_files = sorted(accepted_dir.glob("part-*.parquet"))

    if not parquet_files:
        raise RuntimeError(
            f"No accepted Parquet files found in {accepted_dir}"
        )

    # Use a list of concrete paths for DuckDB's read_parquet function.
    files = [str(path) for path in parquet_files]

    con = duckdb.connect(str(DB_PATH))

    try:
        con.execute(
            """
            CREATE OR REPLACE TABLE fact_incremental_transactions AS
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
            [files],
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE incremental_product_summary AS
            SELECT
                COALESCE(product_code, 'UNKNOWN') AS product_code,
                COUNT(*) AS transaction_count,
                ROUND(SUM(transaction_amount), 2) AS total_amount,
                ROUND(AVG(transaction_amount), 2) AS average_amount,
                SUM(is_fraud) AS fraud_count,
                ROUND(100.0 * AVG(is_fraud), 4) AS fraud_rate_pct
            FROM fact_incremental_transactions
            GROUP BY COALESCE(product_code, 'UNKNOWN')
            """
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE incremental_quality_reconciliation AS
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT transaction_id) AS distinct_ids,
                COUNT(*) - COUNT(DISTINCT transaction_id)
                    AS duplicate_id_count,
                COUNT(*) FILTER (
                    WHERE transaction_id IS NULL
                ) AS null_id_count,
                COUNT(*) FILTER (
                    WHERE transaction_amount IS NULL
                       OR NOT isfinite(transaction_amount)
                       OR transaction_amount < 0
                ) AS invalid_amount_count,
                COUNT(*) FILTER (
                    WHERE is_fraud IS NULL
                       OR is_fraud NOT IN (0, 1)
                ) AS invalid_fraud_label_count
            FROM fact_incremental_transactions
            """
        )

        model_rows = con.execute(
            """
            SELECT total_rows, distinct_ids, duplicate_id_count,
                   null_id_count, invalid_amount_count,
                   invalid_fraud_label_count
            FROM incremental_quality_reconciliation
            """
        ).fetchone()

        columns = [
            "total_rows",
            "distinct_ids",
            "duplicate_id_count",
            "null_id_count",
            "invalid_amount_count",
            "invalid_fraud_label_count",
        ]
        reconciliation = dict(zip(columns, model_rows))

        expected_rows = validation_report["accepted_rows"]

        if reconciliation["total_rows"] != expected_rows:
            raise RuntimeError(
                "Row-count mismatch: "
                f"validation accepted {expected_rows}, "
                f"but DuckDB contains {reconciliation['total_rows']}."
            )

        if reconciliation["duplicate_id_count"] != 0:
            raise RuntimeError("Duplicate transaction IDs found in model.")

        if reconciliation["null_id_count"] != 0:
            raise RuntimeError("Null transaction IDs found in model.")

        if reconciliation["invalid_amount_count"] != 0:
            raise RuntimeError("Invalid transaction amounts found in model.")

        if reconciliation["invalid_fraud_label_count"] != 0:
            raise RuntimeError("Invalid fraud labels found in model.")

        summary_df = con.execute(
            """
            SELECT *
            FROM incremental_product_summary
            ORDER BY transaction_count DESC
            """
        ).df()

        summary_path = REPORT_DIR / "incremental_product_summary.csv"
        summary_df.to_csv(summary_path, index=False)

        reconciliation_path = (
            REPORT_DIR / "incremental_reconciliation.json"
        )
        reconciliation_path.write_text(
            json.dumps(
                {
                    "status": "PASS",
                    "batch_hash": validation_report["batch_hash"],
                    "expected_accepted_rows": expected_rows,
                    "duckdb_reconciliation": reconciliation,
                    "database": str(DB_PATH),
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        print("\nINCREMENTAL MODEL BUILD")
        print("-----------------------")
        print(f"Database: {DB_PATH}")
        print(f"Validated batch: {batch_dir.name}")
        print(f"Rows in model: {reconciliation['total_rows']:,}")
        print(f"Distinct IDs: {reconciliation['distinct_ids']:,}")
        print("Reconciliation: PASS")
        print(f"Product summary: {summary_path}")
        print(f"Reconciliation report: {reconciliation_path}")

        print("\nPRODUCT SUMMARY")
        print(summary_df.to_string(index=False))

    finally:
        con.close()


if __name__ == "__main__":
    main()
