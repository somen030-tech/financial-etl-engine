
CREATE OR REPLACE TABLE data_quality_summary AS
SELECT
    COUNT(*) AS total_rows,

    COUNT(DISTINCT transaction_id)
        AS distinct_transaction_ids,

    SUM(
        CASE WHEN transaction_id IS NULL
        THEN 1 ELSE 0 END
    ) AS null_transaction_ids,

    SUM(
        CASE WHEN transaction_amount < 0
        THEN 1 ELSE 0 END
    ) AS negative_amount_rows,

    SUM(
        CASE WHEN is_fraud NOT IN (0, 1)
                  OR is_fraud IS NULL
        THEN 1 ELSE 0 END
    ) AS invalid_fraud_labels,

    SUM(
        CASE WHEN transaction_amount IS NULL
        THEN 1 ELSE 0 END
    ) AS null_amount_rows

FROM fact_transactions;
