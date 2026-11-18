WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'purchasing_suppliers',
        'supplier_id',
        '_batch_id DESC, valid_from DESC'
    ) }}
)

SELECT
    supplier_id,
    supplier_name,
    supplier_category_id,
    primary_contact_person_id,
    alternate_contact_person_id,
    delivery_method_id,
    delivery_city_id,
    postal_city_id,
    supplier_reference,
    payment_days,
    valid_from,
    valid_to
FROM deduped
