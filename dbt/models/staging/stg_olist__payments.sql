{{ config(materialized='view') }}

-- Payments come in at installment grain (multiple rows per order).
-- We keep that grain here. Order-level rollup happens in fct_orders.

with source as (
    select * from {{ source('olist', 'order_payments') }}
),

renamed as (
    select
        order_id,
        payment_sequential                    as installment_sequence,
        payment_type,
        cast(payment_installments as integer) as installment_count,
        cast(payment_value as decimal(10,2))  as payment_amount
    from source
)

select * from renamed
