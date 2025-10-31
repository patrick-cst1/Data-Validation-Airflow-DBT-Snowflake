{% macro test_value_in_range(model, column_name, min_value, max_value) %}

/*
Test that numeric values fall within expected range.
Usage: {{ test_value_in_range('stg_orders', 'total_amount', 0, 10000) }}
*/

select
    {{ column_name }},
    {{ min_value }} as min_threshold,
    {{ max_value }} as max_threshold
from {{ model }}
where {{ column_name }} < {{ min_value }}
   or {{ column_name }} > {{ max_value }}

{% endmacro %}
