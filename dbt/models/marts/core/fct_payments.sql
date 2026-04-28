{{ config(materialized='table') }}

-- Installment-grain fact. One row per payment installment.
-- Supports analysis of payment methods, installment patterns, BNPL adoption.

with payments as (
    select * from {{ ref('stg_olist__payments') }}
),

orders as (
    select * from {{ ref('stg_olist__orders') }}
),

joined as (
    select
        -- Composite natural key + surrogate
        p.order_id,
        p.installment_sequence,
        {{ dbt_utils.generate_surrogate_key(['p.order_id','p.installment_sequence']) }}
                                                  as payment_key,

        -- Date key (purchase date, since payments don't have their own timestamp)
        cast(o.purchased_at as date)              as order_date_key,

        -- Order context
        o.order_status,

        -- Dimensions
        p.payment_type,
        p.installment_count,

        -- Measures
        p.payment_amount,

        -- Buckets for easier analysis
        case
            when p.installment_count = 1                          then '1 (single)'
            when p.installment_count between 2 and 3              then '2-3'
            when p.installment_count between 4 and 6              then '4-6'
            when p.installment_count between 7 and 12             then '7-12'
            when p.installment_count > 12                         then '13+'
            else 'unknown'
        end                                       as installment_bucket,

        -- Audit
        current_timestamp                         as dbt_loaded_at

    from payments p
    inner join orders o on p.order_id = o.order_id
)

select * from joined
