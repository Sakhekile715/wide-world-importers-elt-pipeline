WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'purchasing_purchaseorders',
        'purchase_order_id',
        '_batch_id DESC, last_edited_when DESC'
    ) }}
)

SELECT
    purchase_order_id,
    supplier_id,
    order_date,
    delivery_method_id,
    contact_person_id,
    expected_delivery_date,
    supplier_reference,
    is_order_finalized,
    comments,
    internal_comments
FROM deduped
