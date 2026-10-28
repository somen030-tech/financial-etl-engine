
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "models" / "financial_analytics.duckdb"


def main():
    con = duckdb.connect(str(DB_PATH), read_only=True)

    try:
        print("\nBATCH STATUS")
        print(con.execute("""
            SELECT
                batch_hash,
                source_row_count,
                loaded_row_count,
                status
            FROM etl_batch_ledger
            ORDER BY loaded_at
        """).df().to_string(index=False))

        print("\nTRANSACTIONS BY SOURCE BATCH")
        print(con.execute("""
            SELECT
                source_batch_hash,
                COUNT(*) AS transaction_count,
                SUM(is_fraud) AS fraud_count,
                ROUND(SUM(transaction_amount), 2) AS total_amount
            FROM fact_transactions_incremental
            GROUP BY source_batch_hash
        """).df().to_string(index=False))

        print("\nOVERALL RECONCILIATION")
        print(con.execute("""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT transaction_id) AS distinct_ids,
                COUNT(*) - COUNT(DISTINCT transaction_id)
                    AS duplicate_ids
            FROM fact_transactions_incremental
        """).df().to_string(index=False))

    finally:
        con.close()


if __name__ == "__main__":
    main()
