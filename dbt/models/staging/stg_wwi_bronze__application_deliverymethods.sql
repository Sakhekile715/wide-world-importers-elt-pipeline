WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'application_deliverymethods',
        'delivery_method_id',
        '_batch_id DESC, valid_from DESC'
    ) }}
)

SELECT
    delivery_method_id,
    delivery_method_name
FROM deduped