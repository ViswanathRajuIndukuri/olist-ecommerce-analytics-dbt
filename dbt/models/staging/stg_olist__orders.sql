{{ config(materialized='view') }}

-- Staging: minimal transformations, rename columns, cast types,
-- and pre-compute simple flags. No joins, no business logic.

with source as (
    select * from {{ source('olist', 'orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        order_status,

        -- Cast all timestamps explicitly
        cast(order_purchase_timestamp      as timestamp) as purchased_at,
        cast(order_approved_at             as timestamp) as approved_at,
        cast(order_delivered_carrier_date  as timestamp) as shipped_at,
        cast(order_delivered_customer_date as timestamp) as delivered_at,
        cast(order_estimated_delivery_date as timestamp) as estimated_delivery_at,

        -- Status flags (cheap to precompute, simplifies downstream)
        case when order_status = 'delivered' then true else false end as is_delivered,
        case when order_status = 'canceled'  then true else false end as is_canceled,
        case when order_status in ('processing','approved','invoiced','shipped','created')
             then true else false end as is_in_progress,

        -- Time-based derivations (null-safe)
        case
            when order_delivered_customer_date is not null
             and order_purchase_timestamp      is not null
            then date_diff(
                'day',
                cast(order_purchase_timestamp as timestamp),
                cast(order_delivered_customer_date as timestamp)
            )
        end as delivery_days_actual,

        case
            when order_estimated_delivery_date is not null
             and order_purchase_timestamp      is not null
            then date_diff(
                'day',
                cast(order_purchase_timestamp as timestamp),
                cast(order_estimated_delivery_date as timestamp)
            )
        end as delivery_days_estimated,

        case
            when order_delivered_customer_date is not null
             and order_estimated_delivery_date is not null
             and cast(order_delivered_customer_date as timestamp)
                 > cast(order_estimated_delivery_date as timestamp)
            then true
            when order_delivered_customer_date is not null
            then false
            else null
        end as is_late_delivery

    from source
)

select * from renamed
