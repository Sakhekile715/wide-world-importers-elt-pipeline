WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'application_transactiontypes',
        'transaction_type_id',
        '_batch_id DESC, valid_from DESC'
    ) }}
)

SELECT
    transaction_type_id,
    transaction_type_name
FROM deduped