{{ config(materialized='view') }}

with source as (
    select * from {{ source('olist', 'order_items') }}
),

renamed as (
    select
        order_id,
        order_item_id                              as item_sequence,
        product_id,
        seller_id,

        cast(shipping_limit_date as timestamp)     as shipping_limit_at,

        cast(price          as decimal(10, 2))     as item_price,
        cast(freight_value  as decimal(10, 2))     as freight_value,
        cast(price          as decimal(10, 2))
            + cast(freight_value as decimal(10, 2)) as total_item_revenue
    from source
)

select * from renamed
