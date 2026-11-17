WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'application_people',
        'person_id',
        '_batch_id DESC, valid_from DESC'
    ) }}
)

SELECT
    person_id,
    full_name,
    is_employee,
    is_salesperson
FROM deduped