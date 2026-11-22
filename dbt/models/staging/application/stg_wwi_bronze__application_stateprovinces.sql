WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'application_stateprovinces',
        'state_province_id',
        '_batch_id DESC, valid_from DESC'
    ) }}
)

SELECT
    state_province_id,
    state_province_name,
    state_province_code,
    country_id,
    sales_territory,   
    latest_recorded_population
FROM deduped