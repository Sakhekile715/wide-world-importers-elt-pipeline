WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'application_countries',
        'country_id',
        '_batch_id DESC, valid_from DESC'
    ) }}
)

SELECT
    country_id,
    country_name,
    formal_name,
    iso_alpha3_code,
    iso_numeric_code,
    country_type,
    latest_recorded_population,
    continent,
    region,
    subregion,
    border
FROM deduped


    