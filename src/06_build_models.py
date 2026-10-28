
from pathlib import Path
import json
import logging

import duckdb


# --------------------------------------------------
# 1. Project configuration
# --------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent

REPORT_DIR = ROOT / "data" / "reports"
SQL_DIR = ROOT / "sql"
LOG_DIR = ROOT / "data" / "logs"
MODEL_DIR = ROOT / "data" / "models"

DB_PATH = MODEL_DIR / "financial_analytics.duckdb"

MODEL_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_DIR / "modeling.log",
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# 2. Locate the latest successful cleaning run
# --------------------------------------------------

def get_cleaned_files():
    reports = list(REPORT_DIR.glob("cleaning_*.json"))

    if not reports:
        raise FileNotFoundError(
            "No cleaning report found. "
            "Run src/04_clean_data.py first."
        )

    latest_report_path = max(
        reports,
        key=lambda p: p.stat().st_mtime,
    )

    report = json.loads(
        latest_report_path.read_text(encoding="utf-8")
    )

    if report.get("status") != "SUCCESS":
        raise ValueError("Latest cleaning run was not successful.")

    output_dir = Path(report["output_directory"])

    files = sorted(output_dir.glob("*.parquet"))

    if not files:
        raise FileNotFoundError(
            f"No cleaned Parquet files found in {output_dir}"
        )

    return latest_report_path, report, files


# --------------------------------------------------
# 3. Execute the SQL models
# --------------------------------------------------

def main():
    report_path, report, parquet_files = get_cleaned_files()

    logger.info("Cleaning report: %s", report_path)
    logger.info("Reading %s Parquet files", len(parquet_files))

    connection = duckdb.connect(str(DB_PATH))

    try:
        # Register the file list as a bound parameter.
        # The paths are not concatenated into SQL strings.
        parameters = {
            "cleaned_files": [str(p) for p in parquet_files]
        }

        sql_files = [
            "01_fact_transactions.sql",
            "02_product_summary.sql",
            "03_quality_summary.sql",
        ]

        for filename in sql_files:
            sql_path = SQL_DIR / filename

            if not sql_path.exists():
                raise FileNotFoundError(
                    f"SQL model not found: {sql_path}"
                )

            query = sql_path.read_text(encoding="utf-8")

            logger.info("Running SQL model: %s", filename)

            if filename == "01_fact_transactions.sql":
                connection.execute(query, parameters)
            else:
                connection.execute(query)

        # Export small analytical summaries for easy inspection.
        summary_path = MODEL_DIR / "product_transaction_summary.csv"

        connection.execute(
            """
            COPY (
                SELECT *
                FROM product_transaction_summary
                ORDER BY transaction_count DESC
            )
            TO ?
            (HEADER, DELIMITER ',')
            """,
            [str(summary_path)],
        )

        # Read back model validation metrics.
        quality = connection.execute(
            "SELECT * FROM data_quality_summary"
        ).fetchdf()

        print("\nDATA QUALITY SUMMARY")
        print(quality.to_string(index=False))

        # Check key integrity conditions.
        result = quality.iloc[0]

        if (
            result["total_rows"]
            != result["distinct_transaction_ids"]
        ):
            raise ValueError(
                "Duplicate or null transaction IDs detected "
                "in the modeled table."
            )

        if (
            result["null_transaction_ids"] > 0
            or result["negative_amount_rows"] > 0
            or result["invalid_fraud_labels"] > 0
            or result["null_amount_rows"] > 0
        ):
            raise ValueError(
                "One or more data quality checks failed."
            )

        print("\nPRODUCT TRANSACTION SUMMARY")
        print(
            connection.execute(
                """
                SELECT *
                FROM product_transaction_summary
                ORDER BY transaction_count DESC
                LIMIT 10
                """
            ).fetchdf().to_string(index=False)
        )

        print("\nDatabase:", DB_PATH)
        print("Summary CSV:", summary_path)
        print("Modeling completed successfully.")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
