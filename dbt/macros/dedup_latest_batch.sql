{% macro dedup_latest_batch(source_name, table_name, partition_by, order_by='_batch_id DESC') %}
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
            ) AS _rn
        FROM {{ source(source_name, table_name) }}
    ) AS ranked
    WHERE _rn = 1
{% endmacro %}