WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'warehouse_stockitemtransactions',
        'stock_item_transaction_id',
        '_batch_id DESC, last_edited_when DESC'
    ) }}
)

SELECT
    stock_item_transaction_id,
    stock_item_id,
    transaction_type_id,
    customer_id,
    invoice_id,
    supplier_id,
    purchase_order_id,
    transaction_occurred_when,
    quantity
FROM deduped
