{{ config(materialized='table') }}

with sellers as (
    select * from {{ ref('stg_olist__sellers') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,
        seller_id,
        zip_code_prefix,
        city,
        state,

        -- Brazilian region rollup
        case
            when state in ('SP','RJ','MG','ES')                          then 'Southeast'
            when state in ('PR','SC','RS')                               then 'South'
            when state in ('GO','MT','MS','DF')                          then 'Center-West'
            when state in ('BA','PE','CE','MA','PB','RN','AL','SE','PI') then 'Northeast'
            when state in ('AM','PA','RO','AC','RR','AP','TO')           then 'North'
            else 'Unknown'
        end                                   as region,

        current_timestamp                     as dbt_loaded_at
    from sellers
)

select * from final
