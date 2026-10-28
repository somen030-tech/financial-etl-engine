
CREATE OR REPLACE TABLE fact_transactions AS
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

    -- Relative time bucket, not a calendar date.
    CAST(FLOOR(TransactionDT / 86400.0) AS BIGINT)
        AS relative_day_bucket

FROM read_parquet($cleaned_files, union_by_name = true);
