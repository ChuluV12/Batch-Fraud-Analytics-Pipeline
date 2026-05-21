WITH ordered_txns AS (
    SELECT
        transaction_id,
        user_id,
        timestamp,
        amount,
        country,
        merchant,
        ROUND(
            (CAST(to_unixtime(timestamp) AS DOUBLE) -
             CAST(to_unixtime(
                 LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp)
             ) AS DOUBLE)) / 60.0,
        2) AS mins_since_last_txn
    FROM fraud_db.processed
),
flagged AS (
    SELECT
        *,
        CASE
            WHEN mins_since_last_txn IS NOT NULL
             AND mins_since_last_txn < 10 THEN TRUE
            ELSE FALSE
        END AS is_rapid_succession
    FROM ordered_txns
)
SELECT
    transaction_id,
    user_id,
    timestamp,
    amount,
    country,
    merchant,
    mins_since_last_txn,
    is_rapid_succession,
    'RAPID_SUCCESSION' AS fraud_reason,
    1 AS is_fraud
FROM flagged
WHERE is_rapid_succession = TRUE
ORDER BY user_id, timestamp;