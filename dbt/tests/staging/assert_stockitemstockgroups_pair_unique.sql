-- A stock item belongs to each stock group at most once. A duplicate pair
-- would double-count the item's sales when reporting by stock group.
--
-- The surrogate key stock_item_stock_group_id is already tested as unique,
-- but that wouldn't catch the same pair stored under two different IDs.

SELECT
    stock_item_id,
    stock_group_id,
    COUNT(*) AS n_rows
FROM {{ ref('stg_wwi_bronze__warehouse_stockitemstockgroups') }}
GROUP BY stock_item_id, stock_group_id
HAVING COUNT(*) > 1
