
import json
import logging
import sys
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]

DB_PATH = ROOT / "data" / "models" / "financial_analytics.duckdb"
LOG_DIR = ROOT / "data" / "logs"
REPORT_DIR = ROOT / "data" / "reports"

LOG_FILE = LOG_DIR / "pipeline_monitor.log"


def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )


def collect_metrics(con):
    """Read the ledger and fact table, then calculate health metrics."""

    ledger_batches, ledger_rows = con.execute("""
        SELECT
            COUNT(*) AS batch_count,
            COALESCE(SUM(loaded_row_count), 0) AS loaded_rows
        FROM etl_batch_ledger
        WHERE status = 'SUCCESS'
    """).fetchone()

    (
        fact_rows,
        distinct_ids,
        null_ids,
        invalid_amounts,
        invalid_labels,
        duplicate_groups,
    ) = con.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT transaction_id) AS distinct_ids,
            COUNT(*) FILTER (
                WHERE transaction_id IS NULL
            ) AS null_ids,
            COUNT(*) FILTER (
                WHERE transaction_amount IS NULL
                   OR NOT isfinite(transaction_amount)
                   OR transaction_amount < 0
            ) AS invalid_amounts,
            COUNT(*) FILTER (
                WHERE is_fraud IS NULL
                   OR is_fraud NOT IN (0, 1)
            ) AS invalid_labels,
            (
                SELECT COUNT(*)
                FROM (
                    SELECT transaction_id
                    FROM fact_transactions_incremental
                    GROUP BY transaction_id
                    HAVING COUNT(*) > 1
                ) AS duplicate_groups
            ) AS duplicate_groups
        FROM fact_transactions_incremental
    """).fetchone()

    return {
        "successful_batches": int(ledger_batches),
        "ledger_loaded_rows": int(ledger_rows),
        "fact_table_rows": int(fact_rows),
        "distinct_transaction_ids": int(distinct_ids),
        "null_transaction_ids": int(null_ids),
        "duplicate_id_groups": int(duplicate_groups),
        "invalid_amount_rows": int(invalid_amounts),
        "invalid_fraud_label_rows": int(invalid_labels),
    }


def evaluate_health(metrics):
    """Return explicit checks so the report explains failures."""
    checks = {
        "at_least_one_successful_batch":
            metrics["successful_batches"] > 0,

        "ledger_matches_fact_table":
            metrics["ledger_loaded_rows"] == metrics["fact_table_rows"],

        "no_null_transaction_ids":
            metrics["null_transaction_ids"] == 0,

        "no_duplicate_transaction_ids":
            metrics["duplicate_id_groups"] == 0,

        "no_invalid_transaction_amounts":
            metrics["invalid_amount_rows"] == 0,

        "no_invalid_fraud_labels":
            metrics["invalid_fraud_label_rows"] == 0,
    }

    return checks


def main():
    setup_logging()
    run_id = uuid.uuid4().hex
    started_at = datetime.now(timezone.utc)
    start_clock = time.perf_counter()

    logging.info("Pipeline monitoring started | run_id=%s", run_id)

    report_path = REPORT_DIR / f"monitor_{run_id}.json"

    report = {
        "run_id": run_id,
        "started_at_utc": started_at.isoformat(),
        "database": str(DB_PATH),
    }

    try:
        if not DB_PATH.exists():
            raise FileNotFoundError(
                f"DuckDB database does not exist: {DB_PATH}. "
                "Run src\\13_load_batches.py first."
            )

        with duckdb.connect(str(DB_PATH), read_only=True) as con:
            tables = {
                row[0]
                for row in con.execute("SHOW TABLES").fetchall()
            }

            required_tables = {
                "etl_batch_ledger",
                "fact_transactions_incremental",
            }

            missing_tables = required_tables - tables
            if missing_tables:
                raise RuntimeError(
                    f"Required database tables missing: "
                    f"{sorted(missing_tables)}"
                )

            metrics = collect_metrics(con)

        checks = evaluate_health(metrics)
        failed_checks = [
            name for name, passed in checks.items() if not passed
        ]

        report["metrics"] = metrics
        report["checks"] = checks
        report["failed_checks"] = failed_checks
        report["status"] = "PASS" if not failed_checks else "FAIL"

        if failed_checks:
            logging.error(
                "Monitoring checks failed: %s",
                ", ".join(failed_checks),
            )
        else:
            logging.info("All monitoring checks passed.")

    except Exception as exc:
        report["status"] = "ERROR"
        report["error"] = str(exc)
        logging.exception("Monitoring could not complete.")

    finally:
        report["finished_at_utc"] = datetime.now(
            timezone.utc
        ).isoformat()
        report["duration_seconds"] = round(
            time.perf_counter() - start_clock, 3
        )

        report_path.write_text(
            json.dumps(report, indent=2),
            encoding="utf-8",
        )

        logging.info("Monitor status: %s", report["status"])
        logging.info("Audit report: %s", report_path)
        logging.info("Duration: %.3f seconds", report["duration_seconds"])

    print("\nMONITORING SUMMARY")
    print("------------------")
    print(f"Status: {report['status']}")
    print(f"Audit report: {report_path}")

    if "metrics" in report:
        for name, value in report["metrics"].items():
            print(f"{name}: {value}")

    if report.get("failed_checks"):
        print("\nFAILED CHECKS:")
        for name in report["failed_checks"]:
            print(f"- {name}")

    if report["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
