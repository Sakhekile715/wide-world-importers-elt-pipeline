-- Same check as assert_every_invoice_has_lines, for sales orders.

{{ config(severity='warn') }}

SELECT o.order_id
FROM {{ ref('stg_wwi_bronze__sales_orders') }} AS o
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('stg_wwi_bronze__sales_orderlines') }} AS l
    WHERE l.order_id = o.order_id
)
