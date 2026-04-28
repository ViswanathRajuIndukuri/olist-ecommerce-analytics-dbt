{{ config(materialized='table') }}

with products as (
    select * from {{ ref('stg_olist__products') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
        product_id,
        category_name,
        category_name_pt,

        -- Bucket by weight for easier slicing
        case
            when weight_g <  500              then 'XS (<0.5kg)'
            when weight_g <  2000             then 'S (0.5–2kg)'
            when weight_g <  5000             then 'M (2–5kg)'
            when weight_g < 15000             then 'L (5–15kg)'
            when weight_g >= 15000            then 'XL (15kg+)'
            else 'unknown'
        end                                   as weight_bucket,

        weight_g,
        length_cm,
        height_cm,
        width_cm,

        -- Volume in cubic cm
        case
            when length_cm is not null
             and height_cm is not null
             and width_cm  is not null
            then length_cm * height_cm * width_cm
        end                                   as volume_cm3,

        photos_qty,
        name_length,
        description_length,
        current_timestamp                     as dbt_loaded_at
    from products
)

select * from final
