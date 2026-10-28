
from pathlib import Path
import duckdb


ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "models" / "financial_analytics.duckdb"

con = duckdb.connect(str(DB_PATH), read_only=True)

try:
    print("\nAvailable tables:")
    print(
        con.execute("SHOW TABLES").fetchdf()
    )

    print("\nTransaction counts by relative day:")
    print(
        con.execute(
            """
            SELECT
                relative_day_bucket,
                COUNT(*) AS transactions,
                ROUND(AVG(transaction_amount), 2) AS avg_amount
            FROM fact_transactions
            GROUP BY relative_day_bucket
            ORDER BY relative_day_bucket
            LIMIT 15
            """
        ).fetchdf()
    )

    print("\nFraud rates by product:")
    print(
        con.execute(
            """
            SELECT
                product_code,
                transaction_count,
                fraud_count,
                fraud_rate_pct
            FROM product_transaction_summary
            ORDER BY fraud_rate_pct DESC NULLS LAST
            """
        ).fetchdf()
    )

finally:
    con.close()
