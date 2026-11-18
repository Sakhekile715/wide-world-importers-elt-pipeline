-- Every stock item should have a stock-on-hand row. A missing one would drop
-- the item from inventory reports instead of showing it with zero stock.
--
-- Warning only: a brand-new item can exist before its first holding row.

{{ config(severity='warn') }}

SELECT si.stock_item_id
FROM {{ ref('stg_wwi_bronze__warehouse_stockitems') }} AS si
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('stg_wwi_bronze__warehouse_stockitemholdings') }} AS h
    WHERE h.stock_item_id = si.stock_item_id
)
