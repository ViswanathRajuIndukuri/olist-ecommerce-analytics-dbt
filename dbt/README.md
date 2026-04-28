# Olist Analytics — dbt Sales & Revenue Models

End-to-end dbt project on the Olist Brazilian e-commerce dataset, focused on the **Sales & Revenue Analytics** domain.

## Architecture

```
load_data/olist.duckdb (raw schema, populated by load_olist.py)
        ↓
staging/    (views — light cleanup, type casting)
        ↓
intermediate/    (ephemeral — reusable rollups)
        ↓
marts/core/      (tables — star schema)
        ↓
marts/semantic/  (MetricFlow — governed metrics)
```

## Models built (Sales & Revenue domain)

### Staging (6 views)
- `stg_olist__orders` — orders with delivery flags
- `stg_olist__order_items` — line items with derived revenue
- `stg_olist__customers` — customer master
- `stg_olist__products` — products with English category names
- `stg_olist__sellers` — sellers
- `stg_olist__payments` — payments at installment grain

### Intermediate (2 ephemeral)
- `int_order_items__rolled_up` — items aggregated to order grain
- `int_payments__rolled_up` — payments aggregated to order grain

### Marts — Core (8 tables)
- `dim_dates` — date spine (one row per day)
- `dim_customers` — customer dimension at customer_unique_id grain
- `dim_products` — product dimension with weight buckets
- `dim_sellers` — seller dimension with Brazilian region rollup
- `fct_orders` — order-grain fact (heart of sales analytics)
- `fct_order_items` — line-item-grain fact
- `fct_payments` — installment-grain payment fact
- `metricflow_time_spine` — time spine for MetricFlow

### Semantic Layer (2 YAML files, 14 metrics)

**Simple metrics:** `revenue`, `item_revenue`, `freight_cost`, `order_count`, `items_sold`, `unique_customers`

**Ratio metrics:** `avg_order_value`, `avg_items_per_order`, `cancellation_rate`, `delivery_completion_rate`, `freight_pct_of_revenue`

**Derived metrics:** `net_revenue`

**Cumulative metrics:** `cumulative_revenue`

## How to run

From this `dbt/` folder:

```bash
# 1. Install packages (dbt_utils)
poetry run dbt deps --profiles-dir .

# 2. Verify connection to your DuckDB warehouse
poetry run dbt debug --profiles-dir .

# 3. Run all models (staging → intermediate → marts)
poetry run dbt run --profiles-dir .

# 4. Run all tests (PK uniqueness, FK relationships, accepted values, etc.)
poetry run dbt test --profiles-dir .

# 5. Generate and serve docs
poetry run dbt docs generate --profiles-dir .
poetry run dbt docs serve --profiles-dir .
```

## Querying the semantic layer

```bash
# Total revenue
poetry run dbt sl query --metrics revenue

# Revenue by month
poetry run dbt sl query --metrics revenue --group-by metric_time__month

# AOV by payment type
poetry run dbt sl query --metrics avg_order_value --group-by orders__payment_type

# Multiple metrics by state
poetry run dbt sl query --metrics revenue,order_count,avg_order_value --group-by orders__order_status
```

## Sample analytical questions this answers

- *What's our monthly revenue trend?*
- *What's the average order value, and how does it vary by payment method?*
- *What's the cancellation rate, and is it getting better or worse?*
- *What % of revenue is freight (i.e., shipping cost burden)?*
- *Which months have the highest order volume?*
- *What's the cumulative revenue over the project's lifetime?*
- *Are multi-payment-method orders larger or smaller than single-method?*
