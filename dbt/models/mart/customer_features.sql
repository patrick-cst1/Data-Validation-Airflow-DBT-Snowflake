{{
  config(
    materialized='table',
    tags=['mart', 'features', 'ml']
  )
}}

with customer_base as (
    select * from {{ ref('stg_customers') }}
),

customer_orders as (
    select
        customer_id,
        count(distinct order_id) as total_orders,
        sum(case when order_status = 'COMPLETED' then 1 else 0 end) as completed_orders,
        sum(case when order_status = 'CANCELLED' then 1 else 0 end) as cancelled_orders,
        sum(total_amount) as total_spend,
        avg(total_amount) as avg_order_value,
        max(order_timestamp) as last_order_date,
        min(order_timestamp) as first_order_date,
        datediff(day, min(order_timestamp), max(order_timestamp)) as customer_lifetime_days
    from {{ ref('stg_orders') }}
    where order_status in ('COMPLETED', 'CANCELLED', 'REFUNDED')
    group by customer_id
),

customer_events as (
    select
        customer_id,
        count(distinct event_id) as total_events,
        sum(case when event_type = 'PAGE_VIEW' then 1 else 0 end) as page_views,
        sum(case when event_type = 'ADD_TO_CART' then 1 else 0 end) as add_to_cart_events,
        sum(case when event_type = 'PURCHASE' then 1 else 0 end) as purchase_events,
        count(distinct session_id) as total_sessions,
        count(distinct date_trunc('day', event_timestamp)) as active_days
    from {{ ref('stg_events') }}
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.email,
        c.first_name,
        c.last_name,
        c.country_code,
        c.city,
        c.customer_segment,
        c.signup_timestamp,
        
        -- Order metrics
        coalesce(o.total_orders, 0) as total_orders,
        coalesce(o.completed_orders, 0) as completed_orders,
        coalesce(o.cancelled_orders, 0) as cancelled_orders,
        coalesce(o.total_spend, 0) as total_spend,
        coalesce(o.avg_order_value, 0) as avg_order_value,
        o.last_order_date,
        o.first_order_date,
        coalesce(o.customer_lifetime_days, 0) as customer_lifetime_days,
        
        -- Event metrics
        coalesce(e.total_events, 0) as total_events,
        coalesce(e.page_views, 0) as page_views,
        coalesce(e.add_to_cart_events, 0) as add_to_cart_events,
        coalesce(e.purchase_events, 0) as purchase_events,
        coalesce(e.total_sessions, 0) as total_sessions,
        coalesce(e.active_days, 0) as active_days,
        
        -- Calculated features for ML
        case 
            when o.last_order_date is null then null
            else datediff(day, o.last_order_date, current_timestamp())
        end as days_since_last_order,
        
        case 
            when o.customer_lifetime_days > 0 
            then o.total_orders::float / o.customer_lifetime_days * 30
            else 0
        end as monthly_order_frequency,
        
        case 
            when e.total_sessions > 0 
            then e.purchase_events::float / e.total_sessions
            else 0
        end as conversion_rate,
        
        case
            when o.total_orders > 0 
            then o.cancelled_orders::float / o.total_orders
            else 0
        end as cancellation_rate,
        
        -- RFM segmentation inputs
        datediff(day, coalesce(o.last_order_date, c.signup_timestamp), current_timestamp()) as recency_days,
        coalesce(o.total_orders, 0) as frequency,
        coalesce(o.total_spend, 0) as monetary_value,
        
        current_timestamp() as _calculated_at
        
    from customer_base c
    left join customer_orders o on c.customer_id = o.customer_id
    left join customer_events e on c.customer_id = e.customer_id
)

select * from final
