{% macro test_data_freshness_hours(model, timestamp_column, max_hours=24) %}

/*
Test that data is fresh (within specified hours from current time).
Usage: {{ test_data_freshness_hours('stg_orders', 'order_timestamp', 24) }}
*/

with freshness_check as (
    select
        max({{ timestamp_column }}) as max_timestamp,
        current_timestamp() as check_timestamp,
        datediff(hour, max({{ timestamp_column }}), current_timestamp()) as hours_since_last_record
    from {{ model }}
)

select
    max_timestamp,
    check_timestamp,
    hours_since_last_record,
    {{ max_hours }} as max_allowed_hours
from freshness_check
where hours_since_last_record > {{ max_hours }}

{% endmacro %}
