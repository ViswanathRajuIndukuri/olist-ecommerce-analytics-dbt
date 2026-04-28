{{ config(materialized='view') }}

-- Joins translation table here since it's tiny (71 rows) and 1:1 reference data.

with products as (
    select * from {{ source('olist', 'products') }}
),

translation as (
    select * from {{ source('olist', 'product_category_name_translation') }}
),

joined as (
    select
        p.product_id,
        coalesce(t.product_category_name_english, p.product_category_name, 'unknown')
                                              as category_name,
        p.product_category_name               as category_name_pt,
        p.product_name_lenght                 as name_length,
        p.product_description_lenght          as description_length,
        p.product_photos_qty                  as photos_qty,
        cast(p.product_weight_g  as decimal(10,2)) as weight_g,
        cast(p.product_length_cm as decimal(10,2)) as length_cm,
        cast(p.product_height_cm as decimal(10,2)) as height_cm,
        cast(p.product_width_cm  as decimal(10,2)) as width_cm
    from products p
    left join translation t
        on p.product_category_name = t.product_category_name
)

select * from joined
