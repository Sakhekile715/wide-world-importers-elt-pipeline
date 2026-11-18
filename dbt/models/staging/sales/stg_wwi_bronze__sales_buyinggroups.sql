WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'sales_buyinggroups',
        'buying_group_id',
        '_batch_id DESC, valid_from DESC'
    ) }}
)

SELECT
    buying_group_id,
    buying_group_name
FROM deduped
