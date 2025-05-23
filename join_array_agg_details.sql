-- I used this file to check the status of a one-to-many join. I really like the 'visual' output generated using ARRAY_AGG and STRUCT - makes an SQL output look like a  pretty dashboard table. Again - this is not useful for a main query but great for understanding how your data is flowing through your joins.

WITH ChargeCounts AS (
  SELECT
    payment_intent_id,
    COUNT(*) AS charge_count,
    ARRAY_AGG(STRUCT(id, created, amount, status)) AS charge_details
  FROM
    `stripe_v2.charge`
  WHERE
    created >= TIMESTAMP('2024-09-01 00:00:00 UTC')
    AND created < TIMESTAMP('2025-05-01 00:00:00 UTC')
  GROUP BY
    payment_intent_id
  HAVING
    charge_count > 1
),
PaymentIntentDetails AS (
  SELECT
    id,
    created,
    amount,
    currency,
    status
  FROM
    `stripe_v2.payment_intent`
  WHERE
    created >= TIMESTAMP('2024-09-01 00:00:00 UTC')
    AND created < TIMESTAMP('2025-05-01 00:00:00 UTC')
)
SELECT
  c.payment_intent_id,
  c.charge_count,
  c.charge_details,
  p.created AS pi_created,
  p.amount AS pi_amount,
  p.currency AS pi_currency,
  p.status AS pi_status
FROM
  ChargeCounts c
LEFT JOIN
  PaymentIntentDetails p
ON
  c.payment_intent_id = p.id
ORDER BY
  c.charge_count DESC,
  c.payment_intent_id;
