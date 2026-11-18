WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'warehouse_stockitemstockgroups',
        'stock_item_stock_group_id',
        '_batch_id DESC, last_edited_when DESC'
    ) }}
)

SELECT
    stock_item_stock_group_id,
    stock_item_id,
    stock_group_id
FROM deduped
