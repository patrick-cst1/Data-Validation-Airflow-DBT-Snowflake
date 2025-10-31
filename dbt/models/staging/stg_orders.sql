{{
  config(
    materialized='view',
    tags=['staging', 'source']
  )
}}

with source_data as (
    select
        order_id,
        customer_id,
        order_date,
        order_status,
        total_amount,
        payment_method,
        shipping_cost,
        discount_amount,
        _loaded_at
    from {{ source('ecommerce', 'orders') }}
),

cleaned as (
    select
        order_id::varchar as order_id,
        customer_id::varchar as customer_id,
        to_timestamp(order_date) as order_timestamp,
        upper(trim(order_status)) as order_status,
        total_amount::decimal(10,2) as total_amount,
        upper(trim(payment_method)) as payment_method,
        coalesce(shipping_cost, 0)::decimal(10,2) as shipping_cost,
        coalesce(discount_amount, 0)::decimal(10,2) as discount_amount,
        _loaded_at,
        
        -- Derived fields
        (total_amount - coalesce(discount_amount, 0) + coalesce(shipping_cost, 0)) as net_amount,
        
        -- Data quality indicators
        case when total_amount < 0 then 1 else 0 end as negative_amount_flag,
        case when order_status not in ('PENDING', 'COMPLETED', 'CANCELLED', 'REFUNDED') 
             then 1 else 0 end as invalid_status_flag
        
    from source_data
)

select * from cleaned
