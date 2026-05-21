WITH user_countries AS (
    -- Step 1: Find the most common country for each user separately
    SELECT
        user_id,
        country,
        COUNT(*) AS country_count,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS rn
    FROM fraud_db.processed
    GROUP BY user_id, country
)
SELECT
    t.user_id,
    -- Volume metrics
    COUNT(*)                                        AS total_transactions,
    SUM(t.amount)                                   AS total_spend,
    AVG(t.amount)                                   AS avg_transaction_amount,
    MAX(t.amount)                                   AS max_transaction_amount,

    -- Fraud signal counts
    SUM(CASE WHEN t.is_fraud        THEN 1 ELSE 0 END) AS confirmed_fraud_count,
    SUM(CASE WHEN t.is_high_value   THEN 1 ELSE 0 END) AS high_value_txn_count,
    SUM(CASE WHEN t.is_foreign_txn  THEN 1 ELSE 0 END) AS foreign_txn_count,

    -- Fraud breakdown by reason
    SUM(CASE WHEN t.fraud_reason = 'HIGH_AMOUNT'       THEN 1 ELSE 0 END) AS cnt_high_amount,
    SUM(CASE WHEN t.fraud_reason = 'FOREIGN_COUNTRY'   THEN 1 ELSE 0 END) AS cnt_foreign_country,
    SUM(CASE WHEN t.fraud_reason = 'HIGH_VALUE_CNP'    THEN 1 ELSE 0 END) AS cnt_high_value_cnp,
    SUM(CASE WHEN t.fraud_reason = 'RAPID_SUCCESSION'  THEN 1 ELSE 0 END) AS cnt_rapid_succession,

    -- Risk score
    (
        SUM(CASE WHEN t.fraud_reason = 'HIGH_AMOUNT'      THEN 3 ELSE 0 END) +
        SUM(CASE WHEN t.fraud_reason = 'FOREIGN_COUNTRY'  THEN 4 ELSE 0 END) +
        SUM(CASE WHEN t.fraud_reason = 'HIGH_VALUE_CNP'   THEN 3 ELSE 0 END) +
        SUM(CASE WHEN t.fraud_reason = 'RAPID_SUCCESSION' THEN 2 ELSE 0 END)
    )                                               AS risk_score,

    -- Most common country (fetched from our CTE)
    uc.country                                      AS primary_country,

    -- Date range
    MIN(t.txn_date)                                 AS first_txn_date,
    MAX(t.txn_date)                                 AS last_txn_date

FROM fraud_db.processed t
LEFT JOIN user_countries uc
    ON t.user_id = uc.user_id AND uc.rn = 1
GROUP BY t.user_id, uc.country
HAVING SUM(CASE WHEN t.is_fraud THEN 1 ELSE 0 END) > 0
ORDER BY risk_score DESC
LIMIT 500;