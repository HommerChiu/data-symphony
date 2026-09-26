{#-
  從 GA4 event_params 取值。staging 先把 event_params 轉成 map（欄位名 params），
  之後用 ga4_param('page_location') 或 ga4_param('ga_session_id', 'int') 取值。
-#}
{% macro ga4_param(key, type='string') -%}
    element_at(params, '{{ key }}').{{ type }}_value
{%- endmacro %}
