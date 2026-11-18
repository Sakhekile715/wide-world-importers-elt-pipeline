{#
    Generic test: fails when the listed columns together are not unique,
    i.e. returns every combination that appears more than once.

    For models whose key spans several columns, where dbt's built-in
    `unique` (one column only) can't be used:

        data_tests:
          - unique_combination_of_columns:
              arguments:
                combination_of_columns: [customer_id, valid_from, valid_to]

    NULLs are grouped together, so two rows that are NULL in the same key
    column count as duplicates. Pair it with not_null on the key columns.

    Same behaviour as dbt_utils.unique_combination_of_columns, written here
    so the project doesn't need a package (and `dbt deps`).
#}
{% test unique_combination_of_columns(model, combination_of_columns) %}

{%- set columns_csv = combination_of_columns | join(', ') %}

SELECT
    {{ columns_csv }},
    COUNT(*) AS n_rows
FROM {{ model }}
GROUP BY {{ columns_csv }}
HAVING COUNT(*) > 1

{% endtest %}
