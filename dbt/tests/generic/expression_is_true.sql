{#
    Generic test: fails on every row where `expression` is false.

    Use case: for row-level business rules that dbt's built-in tests can't
    express (amounts that must add up, values that must be positive, ...):

        data_tests:
          - expression_is_true:
              name: invoicelines_extended_price_adds_up
              arguments:
                expression: "extended_price = quantity * unit_price + tax_amount"
              config:
                where: "transaction_type_id IN (1, 2)"   # optional row filter

    Rows where the expression evaluates to NULL pass, the same way dbt's
    relationships test ignores null keys. Add "col IS NOT NULL AND ..." to the
    expression when a NULL should count as a failure.

    Same behaviour as dbt_utils.expression_is_true, written here so the
    project doesn't need a package (and `dbt deps`) for one test.
#}
{% test expression_is_true(model, expression) %}

SELECT *
FROM {{ model }}
WHERE NOT ({{ expression }})

{% endtest %}
