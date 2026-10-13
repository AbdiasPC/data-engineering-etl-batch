{#
    Use the configured schema name as-is (STAGING, MARTS) instead of dbt's default
    "<target_schema>_<custom_schema>", so models land in the schemas the admin script
    created and granted.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim | upper }}
{%- endmacro %}
