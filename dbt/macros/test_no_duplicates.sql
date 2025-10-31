{% macro test_no_duplicates(model, columns) %}

/*
Test that there are no duplicate rows based on specified columns.
Usage: {{ test_no_duplicates('stg_orders', ['order_id', 'customer_id']) }}
*/

with grouped as (
    select
        {% for col in columns %}
        {{ col }}{{ "," if not loop.last }}
        {% endfor %},
        count(*) as row_count
    from {{ model }}
    group by
        {% for col in columns %}
        {{ col }}{{ "," if not loop.last }}
        {% endfor %}
    having count(*) > 1
)

select * from grouped

{% endmacro %}
