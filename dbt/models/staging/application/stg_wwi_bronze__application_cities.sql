WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'application_cities',
        'city_id',
        '_batch_id DESC, valid_from DESC'
    ) }}
)

SELECT
    city_id,
    city_name,
    state_province_id,
    location,
    latest_recorded_population
FROM deduped