WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'sales_orderlines',
        'order_line_id',
        '_batch_id DESC, last_edited_when DESC'
    ) }}
)

SELECT
    order_line_id,
    order_id,
    stock_item_id,
    description,
    package_type_id,
    quantity,
    unit_price,
    tax_rate,
    picked_quantity,
    picking_completed_when
FROM deduped
