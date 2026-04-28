{{ config(materialized='table') }}

-- Date spine covering Olist's data range.
-- One row per day. Used for time-series joins and fills gaps.

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('start_date') ~ "' as date)",
        end_date="cast('"   ~ var('end_date')   ~ "' as date)"
    ) }}
),

enriched as (
    select
        cast(date_day as date)                      as date_key,
        date_day                                    as full_date,

        -- Date parts
        extract(year      from date_day)            as year,
        extract(quarter   from date_day)            as quarter,
        extract(month     from date_day)            as month,
        extract(week      from date_day)            as iso_week,
        extract(day       from date_day)            as day_of_month,
        extract(dayofweek from date_day)            as day_of_week,
        extract(dayofyear from date_day)            as day_of_year,

        -- Display strings
        strftime(date_day, '%Y-%m')                 as year_month,
        strftime(date_day, '%B')                    as month_name,
        strftime(date_day, '%A')                    as day_name,

        -- Useful flags
        case when extract(dayofweek from date_day) in (0, 6) then true else false end
                                                    as is_weekend,
        date_trunc('week',    date_day)::date       as week_start,
        date_trunc('month',   date_day)::date       as month_start,
        date_trunc('quarter', date_day)::date       as quarter_start,
        date_trunc('year',    date_day)::date       as year_start
    from date_spine
)

select * from enriched
