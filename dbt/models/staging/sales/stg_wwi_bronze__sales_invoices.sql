WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'sales_invoices',
        'invoice_id',
        '_batch_id DESC, last_edited_when DESC'
    ) }}
)

SELECT
    invoice_id,
    customer_id,
    bill_to_customer_id,
    order_id,
    delivery_method_id,
    contact_person_id,
    accounts_person_id,
    salesperson_person_id,
    packed_by_person_id,
    invoice_date,
    customer_purchase_order_number,
    is_credit_note,
    total_dry_items,
    total_chiller_items,
    confirmed_delivery_time
FROM deduped
