WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'purchasing_suppliercategories',
        'supplier_category_id',
        '_batch_id DESC, valid_from DESC'
    ) }}
)

SELECT
    supplier_category_id,
    supplier_category_name
FROM deduped
