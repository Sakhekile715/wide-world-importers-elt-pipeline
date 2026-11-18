WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'purchasing_suppliertransactions',
        'supplier_transaction_id',
        '_batch_id DESC, last_edited_when DESC'
    ) }}
)

SELECT
    supplier_transaction_id,
    supplier_id,
    transaction_type_id,
    purchase_order_id,
    payment_method_id,
    supplier_invoice_number,
    transaction_date,
    amount_excluding_tax,
    tax_amount,
    transaction_amount,
    outstanding_balance,
    finalization_date,
    is_finalized
FROM deduped
