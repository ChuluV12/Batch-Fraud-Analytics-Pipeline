SELECT
    transaction_id,
    user_id,
    timestamp,
    amount,
    merchant,
    merchant_category,
    country              AS fraud_country,
    card_present,
    'FOREIGN_COUNTRY'    AS fraud_reason,
    1                    AS is_fraud,
    COUNT(*) OVER (PARTITION BY user_id) AS total_txns_by_user,
    SUM(CASE WHEN is_foreign_txn THEN 1 ELSE 0 END)
        OVER (PARTITION BY user_id) AS foreign_txns_by_user
FROM fraud_db.processed
WHERE is_foreign_txn = TRUE
ORDER BY user_id, timestamp;