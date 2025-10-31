{% macro test_data_completeness(model, column_name, threshold=0.95) %}

/*
Test that a column has sufficient data completeness (non-null rate).
Usage: {{ test_data_completeness('stg_customers', 'email', 0.95) }}
*/

with validation as (
    select
        count(*) as total_rows,
        count({{ column_name }}) as non_null_rows,
        count({{ column_name }})::float / nullif(count(*), 0) as completeness_rate
    from {{ model }}
)

select
    completeness_rate,
    {{ threshold }} as threshold
from validation
where completeness_rate < {{ threshold }}

{% endmacro %}
