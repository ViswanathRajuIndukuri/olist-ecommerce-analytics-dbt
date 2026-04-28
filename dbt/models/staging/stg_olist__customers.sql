{{ config(materialized='view') }}

with source as (
    select * from {{ source('olist', 'customers') }}
),

renamed as (
    select
        customer_id,                       -- per-order ID
        customer_unique_id,                -- stable person ID (use this for SCD2)
        customer_zip_code_prefix           as zip_code_prefix,
        customer_city                      as city,
        customer_state                     as state
    from source
)

select * from renamed
