-- Test: revenue should never be negative.
-- Sanity check on item pricing logic.

select 
    order_id,
    gross_revenue
from {{ ref('fct_orders') }}
where gross_revenue < 0
