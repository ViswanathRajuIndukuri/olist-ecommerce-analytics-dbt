{{ config(materialized='table') }}

-- MetricFlow requires a time spine model. Just maps to dim_dates.
-- Convention: a model named metricflow_time_spine with a date_day column.

select 
    date_key as date_day
from {{ ref('dim_dates') }}
