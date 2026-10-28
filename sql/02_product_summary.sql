
CREATE OR REPLACE TABLE product_transaction_summary AS
SELECT
    COALESCE(product_code, 'UNKNOWN') AS product_code,

    COUNT(*) AS transaction_count,

    ROUND(
        SUM(transaction_amount),
        2
    ) AS total_transaction_amount,

    ROUND(
        AVG(transaction_amount),
        2
    ) AS average_transaction_amount,

    SUM(is_fraud) AS fraud_count,

    ROUND(
        100.0 * SUM(is_fraud) / NULLIF(COUNT(*), 0),
        4
    ) AS fraud_rate_pct

FROM fact_transactions
GROUP BY COALESCE(product_code, 'UNKNOWN');
