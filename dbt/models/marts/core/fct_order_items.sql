{{ config(materialized='table') }}

-- Line-item-grain fact. One row per (order_id, item_sequence).
-- The most detailed fact table — supports per-product / per-seller / per-line analysis.

with items as (
    select * from {{ ref('stg_olist__order_items') }}
),

orders as (
    select * from {{ ref('stg_olist__orders') }}
),

customers as (
    select * from {{ ref('stg_olist__customers') }}
),

joined as (
    select
        -- Composite natural key + surrogate key
        i.order_id,
        i.item_sequence,
        {{ dbt_utils.generate_surrogate_key(['i.order_id','i.item_sequence']) }} as order_item_key,

        -- Foreign keys (surrogate)
        {{ dbt_utils.generate_surrogate_key(['c.customer_unique_id']) }} as customer_key,
        {{ dbt_utils.generate_surrogate_key(['i.product_id']) }}         as product_key,
        {{ dbt_utils.generate_surrogate_key(['i.seller_id']) }}          as seller_key,

        -- Natural FKs (kept for joinability)
        i.product_id,
        i.seller_id,
        c.customer_unique_id,

        -- Date keys
        cast(o.purchased_at      as date)         as order_date_key,
        cast(o.delivered_at      as date)         as delivery_date_key,
        cast(i.shipping_limit_at as date)         as shipping_limit_date_key,

        -- Order context (denormalized for query speed)
        o.order_status,
        o.is_delivered,
        o.is_late_delivery,

        -- Measures
        i.item_price,
        i.freight_value,
        i.total_item_revenue,

        -- Audit
        current_timestamp                         as dbt_loaded_at

    from items i
    inner join orders    o on i.order_id    = o.order_id
    inner join customers c on o.customer_id = c.customer_id
)

select * from joined
