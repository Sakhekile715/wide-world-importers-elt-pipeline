WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'purchasing_purchaseorderlines',
        'purchase_order_line_id',
        '_batch_id DESC, last_edited_when DESC'
    ) }}
)

SELECT
    purchase_order_line_id,
    purchase_order_id,
    stock_item_id,
    ordered_outers,
    description,
    received_outers,
    package_type_id,
    expected_unit_price_per_outer,
    last_receipt_date,
    is_order_line_finalized
FROM deduped
