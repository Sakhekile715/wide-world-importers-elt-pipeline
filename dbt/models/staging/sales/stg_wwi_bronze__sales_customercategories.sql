WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'sales_customercategories',
        'customer_category_id',
        '_batch_id DESC, valid_from DESC'
    ) }}
)

SELECT
    customer_category_id,
    customer_category_name
FROM deduped
