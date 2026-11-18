WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'warehouse_stockitemholdings',
        'stock_item_id',
        '_batch_id DESC, last_edited_when DESC'
    ) }}
)

SELECT
    stock_item_id,
    quantity_on_hand,
    bin_location,
    last_stocktake_quantity,
    last_cost_price,
    reorder_level,
    target_stock_level
FROM deduped
