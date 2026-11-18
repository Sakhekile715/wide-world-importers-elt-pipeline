-- An invoice header with no lines would show up in invoice counts with zero
-- revenue. Headers and lines are written in the same source transaction, so
-- this points at a partial load or a dedup problem.
--
-- Warning only: nothing breaks downstream, but it should be looked at.

{{ config(severity='warn') }}

SELECT i.invoice_id
FROM {{ ref('stg_wwi_bronze__sales_invoices') }} AS i
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('stg_wwi_bronze__sales_invoicelines') }} AS l
    WHERE l.invoice_id = i.invoice_id
)
