WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'sales_customertransactions',
        'customer_transaction_id',
        '_batch_id DESC, last_edited_when DESC'
    ) }}
)

SELECT
    customer_transaction_id,
    customer_id,
    transaction_type_id,
    invoice_id,
    payment_method_id,
    transaction_date,
    amount_excluding_tax,
    tax_amount,
    transaction_amount,
    outstanding_balance,
    finalization_date,
    is_finalized
FROM deduped
