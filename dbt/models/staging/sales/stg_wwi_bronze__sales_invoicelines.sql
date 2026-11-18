WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'sales_invoicelines',
        'invoice_line_id',
        '_batch_id DESC, last_edited_when DESC'
    ) }}
)

SELECT
    invoice_line_id,
    invoice_id,
    stock_item_id,
    description,
    package_type_id,
    quantity,
    unit_price,
    tax_rate,
    tax_amount,
    line_profit,
    extended_price
FROM deduped
