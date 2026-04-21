{{
    config(
        materialized   = 'incremental',
        unique_key     = 'order_id',
        on_schema_change = 'sync_all_columns'
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}

    {% if is_incremental() %}
        -- Only pick rows newer than last run
        WHERE updated_at > (
            SELECT MAX(updated_at) FROM {{ this }}
        )
    {% endif %}
),

customers AS (
    SELECT
        customer_id,
        full_name,
        customer_segment,
        city,
        state
    FROM {{ ref('stg_customers') }}
),

products AS (
    SELECT
        product_id,
        product_name,
        category,
        subcategory,
        unit_cost,
        margin_pct
    FROM {{ ref('stg_products') }}
)

SELECT
    o.order_id,
    o.order_date,
    DATE_TRUNC('month', o.order_date)              AS order_month,
    DATE_TRUNC('quarter', o.order_date)            AS order_quarter,
    YEAR(o.order_date)                             AS order_year,

    -- Customer fields
    o.customer_id,
    c.full_name                                    AS customer_name,
    c.customer_segment,
    c.city                                         AS customer_city,
    c.state                                        AS customer_state,

    -- Product fields
    o.product_id,
    p.product_name,
    p.category                                     AS product_category,
    p.subcategory                                  AS product_subcategory,

    -- Order metrics
    o.quantity,
    o.unit_price,
    o.discount_pct,
    o.net_revenue,
    ROUND(o.quantity * p.unit_cost, 2)             AS total_cost,
    ROUND(o.net_revenue - 
          (o.quantity * p.unit_cost), 2)           AS gross_profit,
    ROUND((o.net_revenue - (o.quantity * p.unit_cost))
          / NULLIF(o.net_revenue, 0) * 100, 2)     AS gross_profit_pct,

    -- Order details
    o.order_status,
    o.channel,
    o.region,
    o.created_at,
    o.updated_at

FROM     orders    o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN products  p ON o.product_id  = p.product_id