{{ config(materialized='table') }}

-- Order-grain fact. One row per order.
-- Joins customer info and aggregates line items + payments to order grain.

with orders as (
    select * from {{ ref('stg_olist__orders') }}
),

customers as (
    select * from {{ ref('stg_olist__customers') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

items_rolled as (
    select * from {{ ref('int_order_items__rolled_up') }}
),

payments_rolled as (
    select * from {{ ref('int_payments__rolled_up') }}
),

joined as (
    select
        -- Surrogate + natural keys
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_key,
        o.order_id,

        -- Foreign keys
        dc.customer_key,
        c.customer_unique_id,
        cast(o.purchased_at as date)              as order_date_key,

        -- Status & flags
        o.order_status,
        o.is_delivered,
        o.is_canceled,
        o.is_in_progress,
        o.is_late_delivery,

        -- Timestamps
        o.purchased_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_at,

        -- Derived durations
        o.delivery_days_actual,
        o.delivery_days_estimated,
        case
            when o.delivery_days_actual is not null
             and o.delivery_days_estimated is not null
            then o.delivery_days_actual - o.delivery_days_estimated
        end                                       as delivery_days_vs_estimate,

        -- Item-level rollups (from int_order_items__rolled_up)
        coalesce(i.item_count, 0)                 as item_count,
        coalesce(i.distinct_product_count, 0)     as distinct_product_count,
        coalesce(i.distinct_seller_count, 0)      as distinct_seller_count,
        i.gross_item_revenue,
        i.total_freight,
        i.gross_revenue,
        i.avg_item_price,

        -- Payment rollups (from int_payments__rolled_up)
        p.total_payment_amount,
        p.max_installments,
        p.primary_payment_type,
        p.is_multi_payment_method,

        -- Audit
        current_timestamp                         as dbt_loaded_at

    from orders o
    left join customers       c  on o.customer_id        = c.customer_id
    left join dim_customers   dc on c.customer_unique_id = dc.customer_unique_id
    left join items_rolled    i  on o.order_id           = i.order_id
    left join payments_rolled p  on o.order_id           = p.order_id
)

select * from joined
