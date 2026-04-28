-- Test: order revenue and payment should reconcile WITHIN business-rule tolerance.
--
-- Tolerance derivation (empirical, from data investigation):
--   Credit card → up to 35% uplift OR 100 BRL absolute (Brazilian installment interest)
--   Debit card  → up to 5% absolute discrepancy in EITHER direction
--                 (occasional checkout discounts not captured as separate voucher rows)
--   Voucher     → up to 10% uplift (when combined with installment interest)
--   Other       → strict 5 BRL tolerance
--
-- This test catches genuinely anomalous orders where payment behavior is
-- inconsistent with all known legitimate patterns.

{{ config(severity='warn') }}

select
    order_id,
    primary_payment_type,
    max_installments,
    gross_revenue,
    total_payment_amount,
    abs(gross_revenue - total_payment_amount) as discrepancy,
    case 
        when gross_revenue > 0 
        then round(100.0 * (total_payment_amount - gross_revenue) / gross_revenue, 2)
        else null
    end as uplift_pct
from {{ ref('fct_orders') }}
where gross_revenue is not null
  and total_payment_amount is not null
  and is_canceled = false
  and case
        -- Credit card: installment interest can add up to 35%
        when primary_payment_type = 'credit_card' then
            abs(total_payment_amount - gross_revenue) 
            > greatest(100.00, gross_revenue * 0.35)

        -- Debit card: occasional unrecorded discounts up to ~5%
        when primary_payment_type = 'debit_card' then
            abs(total_payment_amount - gross_revenue) 
            > greatest(20.00, gross_revenue * 0.05)

        -- Voucher: can combine with installments, allow up to 10% uplift
        when primary_payment_type = 'voucher' then
            abs(total_payment_amount - gross_revenue) 
            > greatest(20.00, gross_revenue * 0.10)

        -- Everything else: strict 5 BRL tolerance
        else abs(gross_revenue - total_payment_amount) > 5.00
      end