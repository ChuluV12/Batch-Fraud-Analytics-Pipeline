SELECT
    transaction_id,
    user_id,
    timestamp,
    amount,
    merchant,
    merchant_category,
    country,
    card_present,
    'HIGH_AMOUNT' AS fraud_reason,
    1 AS is_fraud
FROM fraud_db.processed
WHERE amount > 5000
ORDER BY amount DESC;