{#- 直接用 +schema 設定的名字（staging / marts），不要 dbt 預設的 <target>_<schema> -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
