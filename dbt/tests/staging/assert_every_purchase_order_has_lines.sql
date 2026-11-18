-- Same check as assert_every_invoice_has_lines, for purchase orders.

{{ config(severity='warn') }}

SELECT po.purchase_order_id
FROM {{ ref('stg_wwi_bronze__purchasing_purchaseorders') }} AS po
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('stg_wwi_bronze__purchasing_purchaseorderlines') }} AS l
    WHERE l.purchase_order_id = po.purchase_order_id
)
