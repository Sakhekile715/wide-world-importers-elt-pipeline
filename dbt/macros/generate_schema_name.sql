{#
    Use a model's +schema exactly as written ("silver", "gold") instead of
    dbt's default "<target schema>_<custom schema>" (e.g. "silver_gold").
    Models without +schema go to the target schema from profiles.yml.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
