{% macro calculate_data_quality_score(model) %}

/*
Calculate overall data quality score for a model based on multiple dimensions.
Returns a score between 0 and 1.
*/

with row_counts as (
    select count(*) as total_rows
    from {{ model }}
),

null_checks as (
    select
        {% for col in adapter.get_columns_in_relation(model) %}
        sum(case when {{ col.name }} is null then 1 else 0 end) as {{ col.name }}_nulls{{ "," if not loop.last }}
        {% endfor %}
    from {{ model }}
),

quality_metrics as (
    select
        total_rows,
        {% set ns = namespace(sum='') %}
        {% for col in adapter.get_columns_in_relation(model) %}
            {% if ns.sum != '' %}
                {% set ns.sum = ns.sum ~ ' + ' %}
            {% endif %}
            {% set ns.sum = ns.sum ~ col.name ~ '_nulls' %}
        {% endfor %}
        ({{ ns.sum }})::float as total_nulls,
        {{ adapter.get_columns_in_relation(model) | length }} * total_rows as total_cells
    from null_checks
    cross join row_counts
)

select
    total_rows,
    total_nulls,
    total_cells,
    1 - (total_nulls / nullif(total_cells, 0)) as completeness_score,
    case
        when completeness_score >= 0.95 then 'EXCELLENT'
        when completeness_score >= 0.90 then 'GOOD'
        when completeness_score >= 0.80 then 'FAIR'
        else 'POOR'
    end as quality_rating
from quality_metrics

{% endmacro %}
