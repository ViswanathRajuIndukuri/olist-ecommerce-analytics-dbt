{{ config(materialized='ephemeral') }}

-- Aggregates payments to order grain. Each order can have multiple installments
-- and payment types — we summarize them here.

with payments_ranked as (
    select
        order_id,
        payment_type,
        installment_count,
        payment_amount,
        -- For finding the dominant payment type (highest value per order)
        row_number() over (
            partition by order_id 
            order by payment_amount desc
        ) as rn
    from {{ ref('stg_olist__payments') }}
),

aggregated as (
    select
        order_id,
        sum(payment_amount)                  as total_payment_amount,
        count(*)                             as payment_record_count,
        max(installment_count)               as max_installments,
        -- Boolean: did the order use multiple payment methods?
        case when count(distinct payment_type) > 1 then true else false end
                                             as is_multi_payment_method
    from {{ ref('stg_olist__payments') }}
    group by order_id
),

primary_method as (
    select
        order_id,
        payment_type as primary_payment_type
    from payments_ranked
    where rn = 1
)

select 
    a.*,
    p.primary_payment_type
from aggregated a
left join primary_method p on a.order_id = p.order_id
