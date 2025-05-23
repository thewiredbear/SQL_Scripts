WITH FilteredCharges AS (
  SELECT
    payment_intent_id,
    created
  FROM
    `stripe_v2.charge`
  WHERE
    created >= TIMESTAMP('2024-09-01 00:00:00 UTC')
    AND created < TIMESTAMP('2025-05-01 00:00:00 UTC')
),
FilteredPaymentIntents AS (
  SELECT
    id
  FROM
    `stripe_v2.payment_intent`
  WHERE
    created >= TIMESTAMP('2024-09-01 00:00:00 UTC')
    AND created < TIMESTAMP('2025-05-01 00:00:00 UTC')
),
JoinAnalysis AS (
  SELECT
    c.payment_intent_id AS charge_pi_id,
    p.id AS pi_id,
    CASE
      WHEN c.payment_intent_id IS NOT NULL AND p.id IS NOT NULL THEN 'matched'
      WHEN c.payment_intent_id IS NOT NULL AND p.id IS NULL THEN 'charges_only'
      WHEN c.payment_intent_id IS NULL AND p.id IS NOT NULL THEN 'pi_only'
    END AS record_type
  FROM
    FilteredCharges c
  FULL OUTER JOIN
    FilteredPaymentIntents p
  ON
    c.payment_intent_id = p.id
)
SELECT
  COUNTIF(record_type = 'matched') AS inner_join_count,
  COUNTIF(record_type IN ('matched', 'charges_only')) AS left_join_count,
  COUNTIF(record_type IN ('matched', 'pi_only')) AS right_join_count,
  COUNTIF(record_type = 'matched') AS matched_records,
  COUNTIF(record_type = 'charges_only') AS charges_only_records,
  COUNTIF(record_type = 'pi_only') AS pi_only_records,
  COUNT(*) AS total_rows_considered,
  ROUND(COUNTIF(record_type = 'matched') / NULLIF(COUNT(*), 0) * 100, 2) AS matched_percentage,
  ROUND(COUNTIF(record_type = 'charges_only') / NULLIF(COUNT(*), 0) * 100, 2) AS charges_only_percentage,
  ROUND(COUNTIF(record_type = 'pi_only') / NULLIF(COUNT(*), 0) * 100, 2) AS pi_only_percentage,
  (SELECT COUNT(*) FROM FilteredCharges) AS total_charges_rows,
  (SELECT COUNT(*) FROM FilteredPaymentIntents) AS total_pi_rows
FROM
  JoinAnalysis;
