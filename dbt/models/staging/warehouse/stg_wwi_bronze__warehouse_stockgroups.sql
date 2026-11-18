WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'warehouse_stockgroups',
        'stock_group_id',
        '_batch_id DESC, valid_from DESC'
    ) }}
)

SELECT
    stock_group_id,
    stock_group_name
FROM deduped
