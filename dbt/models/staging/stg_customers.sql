{{
  config(
    materialized='view',
    tags=['staging', 'source']
  )
}}

with source_data as (
    select
        customer_id,
        email,
        first_name,
        last_name,
        date_of_birth,
        country,
        city,
        signup_date,
        customer_segment,
        _loaded_at
    from {{ source('ecommerce', 'customers') }}
),

cleaned as (
    select
        customer_id::varchar as customer_id,
        lower(trim(email)) as email,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        to_date(date_of_birth) as date_of_birth,
        upper(trim(country)) as country_code,
        trim(city) as city,
        to_timestamp(signup_date) as signup_timestamp,
        customer_segment,
        _loaded_at,
        
        -- Data quality indicators
        case when email is null then 1 else 0 end as missing_email,
        case when first_name is null or last_name is null then 1 else 0 end as missing_name,
        case when date_of_birth is null then 1 else 0 end as missing_dob
        
    from source_data
)

select * from cleaned
