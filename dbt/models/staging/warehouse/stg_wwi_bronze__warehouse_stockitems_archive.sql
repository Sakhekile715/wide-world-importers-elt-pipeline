WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'warehouse_stockitems_archive',
        'stock_item_id, valid_from, valid_to',
        '_batch_id DESC'
    ) }}
)

SELECT
    stock_item_id,
    stock_item_name,
    supplier_id,
    color_id,
    unit_package_id,
    outer_package_id,
    brand,
    size,
    lead_time_days,
    quantity_per_outer,
    is_chiller_stock,
    tax_rate,
    unit_price,
    recommended_retail_price,
    typical_weight_per_unit,
    valid_from,
    valid_to
FROM deduped
