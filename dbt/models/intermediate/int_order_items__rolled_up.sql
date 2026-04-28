{{ config(materialized='ephemeral') }}

-- Aggregates order_items to order grain.
-- Reused by fct_orders. Materialized as ephemeral (compiled inline).

select
    order_id,
    count(*)                            as item_count,
    count(distinct product_id)          as distinct_product_count,
    count(distinct seller_id)           as distinct_seller_count,
    sum(item_price)                     as gross_item_revenue,
    sum(freight_value)                  as total_freight,
    sum(total_item_revenue)             as gross_revenue,
    avg(item_price)                     as avg_item_price,
    max(item_price)                     as max_item_price,
    min(item_price)                     as min_item_price
from {{ ref('stg_olist__order_items') }}
group by order_id
