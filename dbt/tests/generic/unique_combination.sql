{#- 不想為了一個測試引入 dbt_utils，自己寫一個多欄位 unique 測試 -#}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
