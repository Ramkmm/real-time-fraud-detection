SELECT
  user_id,
  COUNT(*) AS fraud_attempts,
  MAX(timestamp) AS last_attempt
FROM {{ ref('staging_transactions') }}
WHERE is_fraud = 1
GROUP BY user_id
"""
}
