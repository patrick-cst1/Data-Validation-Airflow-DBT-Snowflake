{{
  config(
    materialized='table',
    tags=['mart', 'metrics', 'monitoring']
  )
}}

with daily_orders as (
    select
        date_trunc('day', order_timestamp) as order_date,
        count(distinct order_id) as total_orders,
        count(distinct customer_id) as unique_customers,
        sum(total_amount) as total_revenue,
        avg(total_amount) as avg_order_value,
        sum(case when order_status = 'COMPLETED' then 1 else 0 end) as completed_orders,
        sum(case when order_status = 'CANCELLED' then 1 else 0 end) as cancelled_orders,
        
        -- Data quality metrics
        sum(case when negative_amount_flag = 1 then 1 else 0 end) as negative_amount_count,
        sum(case when invalid_status_flag = 1 then 1 else 0 end) as invalid_status_count,
        count(distinct case when customer_id is null then order_id end) as missing_customer_count
        
    from {{ ref('stg_orders') }}
    group by date_trunc('day', order_timestamp)
),

daily_events as (
    select
        date_trunc('day', event_timestamp) as event_date,
        count(distinct event_id) as total_events,
        count(distinct customer_id) as active_users,
        count(distinct session_id) as total_sessions,
        
        -- Data quality metrics
        sum(case when invalid_event_type_flag = 1 then 1 else 0 end) as invalid_event_type_count,
        count(distinct case when customer_id is null then event_id end) as missing_customer_count
        
    from {{ ref('stg_events') }}
    group by date_trunc('day', event_timestamp)
),

final as (
    select
        coalesce(o.order_date, e.event_date) as metric_date,
        
        -- Order metrics
        coalesce(o.total_orders, 0) as total_orders,
        coalesce(o.unique_customers, 0) as ordering_customers,
        coalesce(o.total_revenue, 0) as total_revenue,
        coalesce(o.avg_order_value, 0) as avg_order_value,
        coalesce(o.completed_orders, 0) as completed_orders,
        coalesce(o.cancelled_orders, 0) as cancelled_orders,
        
        -- Event metrics
        coalesce(e.total_events, 0) as total_events,
        coalesce(e.active_users, 0) as active_users,
        coalesce(e.total_sessions, 0) as total_sessions,
        
        -- Data quality scores
        case 
            when o.total_orders > 0 
            then 1 - (o.negative_amount_count::float / o.total_orders)
            else 1
        end as order_amount_quality_score,
        
        case 
            when o.total_orders > 0 
            then 1 - (o.invalid_status_count::float / o.total_orders)
            else 1
        end as order_status_quality_score,
        
        case 
            when e.total_events > 0 
            then 1 - (e.invalid_event_type_count::float / e.total_events)
            else 1
        end as event_type_quality_score,
        
        current_timestamp() as _calculated_at
        
    from daily_orders o
    full outer join daily_events e on o.order_date = e.event_date
)

select * from final
order by metric_date desc
