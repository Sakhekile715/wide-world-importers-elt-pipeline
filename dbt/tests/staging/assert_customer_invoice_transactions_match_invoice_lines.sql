-- Revenue reconciliation: the customer ledger and the invoice lines must agree.
--
-- Each customer invoice transaction (type 1) records the invoice total. That
-- total must equal the sum of the invoice's line extended prices, or a report
-- built on the ledger and one built on invoice lines would show different
-- revenue for the same invoice.
--
-- Returns one row per invoice whose totals differ (the test fails on any row).

WITH line_totals AS (
    SELECT
        invoice_id,
        SUM(extended_price) AS lines_total
    FROM {{ ref('stg_wwi_bronze__sales_invoicelines') }}
    GROUP BY invoice_id
)

SELECT
    ct.customer_transaction_id,
    ct.invoice_id,
    ct.transaction_amount,
    lt.lines_total
FROM {{ ref('stg_wwi_bronze__sales_customertransactions') }} AS ct
LEFT JOIN line_totals AS lt
    ON lt.invoice_id = ct.invoice_id
WHERE ct.transaction_type_id = 1
  AND ct.transaction_amount IS DISTINCT FROM lt.lines_total
