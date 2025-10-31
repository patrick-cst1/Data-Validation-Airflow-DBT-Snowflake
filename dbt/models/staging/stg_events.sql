{{
  config(
    materialized='view',
    tags=['staging', 'source']
  )
}}

with source_data as (
    select
        event_id,
        customer_id,
        event_type,
        event_timestamp,
        page_url,
        product_id,
        session_id,
        device_type,
        _loaded_at
    from {{ source('ecommerce', 'events') }}
),

cleaned as (
    select
        event_id::varchar as event_id,
        customer_id::varchar as customer_id,
        upper(trim(event_type)) as event_type,
        to_timestamp(event_timestamp) as event_timestamp,
        trim(page_url) as page_url,
        product_id::varchar as product_id,
        session_id::varchar as session_id,
        upper(trim(device_type)) as device_type,
        _loaded_at,
        
        -- Data quality indicators
        case when event_type not in ('PAGE_VIEW', 'ADD_TO_CART', 'PURCHASE', 'SEARCH', 'CLICK')
             then 1 else 0 end as invalid_event_type_flag
        
    from source_data
)

select * from cleaned
