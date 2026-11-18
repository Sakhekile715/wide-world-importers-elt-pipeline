-- The person credited with an invoice must be flagged as a salesperson.
-- The relationships test on salesperson_person_id only proves the person
-- exists; this also checks the flag, so sales-by-salesperson reports don't
-- pick up warehouse staff or customer contacts.
--
-- Warning only: someone could lose the flag after leaving sales, which would
-- be a valid source change rather than a pipeline fault.

{{ config(severity='warn') }}

SELECT
    i.invoice_id,
    i.salesperson_person_id
FROM {{ ref('stg_wwi_bronze__sales_invoices') }} AS i
INNER JOIN {{ ref('stg_wwi_bronze__application_people') }} AS p
    ON p.person_id = i.salesperson_person_id
WHERE NOT p.is_salesperson
