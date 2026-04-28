{{ config(materialized='table') }}

-- Customer dimension at customer_unique_id grain (the stable identity).
-- One row per distinct person.

with customers as (
    select * from {{ ref('stg_olist__customers') }}
),

deduped as (
    -- Collapse multiple per-order customer_id records into one per unique person
    select
        customer_unique_id,
        max(zip_code_prefix)             as zip_code_prefix,
        max(city)                        as city,
        max(state)                       as state,
        count(distinct customer_id)      as order_account_count
    from customers
    group by customer_unique_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }} as customer_key,
        customer_unique_id,
        zip_code_prefix,
        city,
        state,
        order_account_count,
        current_timestamp                as dbt_loaded_at
    from deduped
)

select * from final
