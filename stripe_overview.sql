--developing a stripe overview from replicated stripe database in bigqyuery

-- we have to use MAX() because still we will have multiple balance transaction for charges and have to manage them accordingly.

WITH latest_charge_cte AS (
  -- Select the latest charge per payment intent
  SELECT
    c.id AS charge_id,
    c.payment_intent_id,
    c.amount AS charge_amount,
    c.description AS charge_description,
    c.failure_message,
    c.outcome_seller_message,
    c.status AS charge_status,
    c.created AS charge_created,
    ROW_NUMBER() OVER (PARTITION BY c.payment_intent_id ORDER BY c.created DESC) AS rn
  FROM `stripe_v2.charge` c
  WHERE c.created >= TIMESTAMP('2024-09-01 00:00:00 UTC')
    AND c.payment_intent_id IS NOT NULL
),
latest_refund_cte AS (
  -- Select the latest refund per charge
  SELECT
    r.id AS refund_id,
    r.charge_id,
    r.amount AS refund_amount,
    r.created AS refund_created,
    r.status AS refund_status,
    r.reason AS refund_reason,
    r.description AS refund_description,
    r.balance_transaction_id AS refund_balance_transaction_id,
    ROW_NUMBER() OVER (PARTITION BY r.charge_id ORDER BY r.created DESC) AS rn
  FROM `stripe_v2.refund` r
  WHERE r.created >= TIMESTAMP('2024-09-01 00:00:00 UTC')
),
latest_dispute_cte AS (
  -- Select the latest dispute per charge
  SELECT
    d.id AS dispute_id,
    d.charge_id,
    d.amount AS dispute_amount,
    d.created AS dispute_created,
    d.status AS dispute_status,
    d.reason AS dispute_reason,
    d.evidence_details_due_by AS dispute_evidence_due_by,
    d.is_charge_refundable AS dispute_is_charge_refundable,
    d.balance_transaction AS dispute_balance_transaction_id,
    ROW_NUMBER() OVER (PARTITION BY d.charge_id ORDER BY d.created DESC) AS rn
  FROM `stripe_v2.dispute` d
  WHERE d.created >= TIMESTAMP('2024-09-01 00:00:00 UTC')
),
charge_balance_refund_dispute_cte AS (
  -- Join latest charges with balance transactions, refunds, and disputes
  SELECT
    lc.charge_id,
    lc.payment_intent_id,
    lc.charge_amount,
    lc.charge_description,
    lc.failure_message,
    lc.outcome_seller_message,
    lc.charge_status,
    bt.id AS balance_transaction_id,
    bt.available_on,
    bt.created AS balance_transaction_created,
    bt.description AS balance_transaction_description,
    bt.source,
    bt.fee,
    bt.net,
    bt.status AS balance_transaction_status,
    bt.type AS balance_transaction_type,
    bt.reporting_category,
    lr.refund_id,
    lr.refund_amount,
    lr.refund_created,
    lr.refund_status,
    lr.refund_reason,
    lr.refund_description,
    lr.refund_balance_transaction_id,
    ld.dispute_id,
    ld.dispute_amount,
    ld.dispute_created,
    ld.dispute_status,
    ld.dispute_reason,
    ld.dispute_evidence_due_by,
    ld.dispute_is_charge_refundable,
    ld.dispute_balance_transaction_id
  FROM latest_charge_cte lc
  LEFT JOIN `stripe_v2.balance_transaction` bt
    ON lc.charge_id = bt.source
    AND bt.type != 'reserved_funds'
    AND bt.created >= TIMESTAMP('2024-09-01 00:00:00 UTC')
  LEFT JOIN latest_refund_cte lr
    ON lc.charge_id = lr.charge_id
    AND lr.rn = 1
  LEFT JOIN latest_dispute_cte ld
    ON lc.charge_id = ld.charge_id
    AND ld.rn = 1
  WHERE lc.rn = 1
),
pivoted_balance_cte AS (
  -- Pivot balance transactions and include refund and dispute fields
  SELECT
    charge_id,
    payment_intent_id,
    charge_amount,
    charge_description,
    failure_message,
    outcome_seller_message,
    charge_status,
    -- Fields for type = 'payment'
    MAX(CASE WHEN balance_transaction_type != 'payment_failure_refund' THEN balance_transaction_id END) AS payment_id,
    MAX(CASE WHEN balance_transaction_type != 'payment_failure_refund' THEN available_on END) AS payment_available_on,
    MAX(CASE WHEN balance_transaction_type != 'payment_failure_refund' THEN balance_transaction_created END) AS payment_created,
    MAX(CASE WHEN balance_transaction_type != 'payment_failure_refund' THEN balance_transaction_description END) AS payment_description,
    MAX(CASE WHEN balance_transaction_type != 'payment_failure_refund' THEN source END) AS payment_source,
    MAX(CASE WHEN balance_transaction_type != 'payment_failure_refund' THEN fee END) AS payment_fee,
    MAX(CASE WHEN balance_transaction_type != 'payment_failure_refund' THEN net END) AS payment_net,
    MAX(CASE WHEN balance_transaction_type != 'payment_failure_refund' THEN balance_transaction_status END) AS payment_status,
    MAX(CASE WHEN balance_transaction_type != 'payment_failure_refund' THEN reporting_category END) AS payment_reporting_category,
    -- Fields for type = 'payment_failure_refund'
    MAX(CASE WHEN balance_transaction_type = 'payment_failure_refund' THEN balance_transaction_id END) AS payment_failure_refunded_id,
    MAX(CASE WHEN balance_transaction_type = 'payment_failure_refund' THEN available_on END) AS payment_failure_refunded_available_on,
    MAX(CASE WHEN balance_transaction_type = 'payment_failure_refund' THEN balance_transaction_created END) AS payment_failure_refunded_created,
    MAX(CASE WHEN balance_transaction_type = 'payment_failure_refund' THEN balance_transaction_description END) AS payment_failure_refunded_description,
    MAX(CASE WHEN balance_transaction_type = 'payment_failure_refund' THEN source END) AS payment_failure_refunded_source,
    MAX(CASE WHEN balance_transaction_type = 'payment_failure_refund' THEN fee END) AS payment_failure_refunded_fee,
    MAX(CASE WHEN balance_transaction_type = 'payment_failure_refund' THEN net END) AS payment_failure_refunded_net,
    MAX(CASE WHEN balance_transaction_type = 'payment_failure_refund' THEN balance_transaction_status END) AS payment_failure_refunded_status,
    MAX(CASE WHEN balance_transaction_type = 'payment_failure_refund' THEN reporting_category END) AS payment_failure_refunded_reporting_category,
    -- Refund fields
    MAX(refund_id) AS refund_id,
    MAX(refund_amount) AS refund_amount,
    MAX(refund_created) AS refund_created,
    MAX(refund_status) AS refund_status,
    MAX(refund_reason) AS refund_reason,
    MAX(refund_description) AS refund_description,
    MAX(refund_balance_transaction_id) AS refund_balance_transaction_id,
    -- Dispute fields
    MAX(dispute_id) AS dispute_id,
    MAX(dispute_amount) AS dispute_amount,
    MAX(dispute_created) AS dispute_created,
    MAX(dispute_status) AS dispute_status,
    MAX(dispute_reason) AS dispute_reason,
    MAX(dispute_evidence_due_by) AS dispute_evidence_due_by,
    MAX(dispute_is_charge_refundable) AS dispute_is_charge_refundable,
    MAX(dispute_balance_transaction_id) AS dispute_balance_transaction_id
  FROM charge_balance_refund_dispute_cte
  GROUP BY
    charge_id,
    payment_intent_id,
    charge_amount,
    charge_description,
    failure_message,
    outcome_seller_message,
    charge_status
)
-- Main query: Join with payment_intent
SELECT
  pi.id AS payment_intent_id,
  pi.payment_method_types,
  pbc.charge_id,
  pbc.payment_intent_id AS charge_payment_intent_id,
  pbc.charge_amount,
  pbc.charge_description,
  pbc.failure_message,
  pbc.outcome_seller_message,
  pbc.charge_status,
  pbc.payment_id AS balance_transaction_id,
  pbc.payment_available_on AS available_on,
  pbc.payment_created,
  pbc.payment_description AS balance_transaction_description,
  pbc.payment_source AS source,
  pbc.payment_fee AS fee,
  pbc.payment_net AS net,
  pbc.payment_status AS balance_transaction_status,
  pbc.payment_reporting_category AS reporting_category,
  pbc.payment_failure_refunded_id,
  pbc.payment_failure_refunded_available_on,
  pbc.payment_failure_refunded_created,
  pbc.payment_failure_refunded_description,
  pbc.payment_failure_refunded_source,
  pbc.payment_failure_refunded_fee,
  pbc.payment_failure_refunded_net,
  pbc.payment_failure_refunded_status,
  pbc.payment_failure_refunded_reporting_category,
  pbc.refund_id,
  pbc.refund_amount,
  pbc.refund_created,
  pbc.refund_status,
  pbc.refund_reason,
  pbc.refund_description,
  pbc.refund_balance_transaction_id,
  pbc.dispute_id,
  pbc.dispute_amount,
  pbc.dispute_created,
  pbc.dispute_status,
  pbc.dispute_reason,
  pbc.dispute_evidence_due_by,
  pbc.dispute_is_charge_refundable,
  pbc.dispute_balance_transaction_id
FROM `stripe_v2.payment_intent` pi
LEFT JOIN pivoted_balance_cte pbc
  ON pi.id = pbc.payment_intent_id
WHERE pi.created >= TIMESTAMP('2024-09-01 00:00:00 UTC')
ORDER BY pi.id;
