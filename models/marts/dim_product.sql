{{
    config(materialized = 'table')
}}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

sales_stats AS (
    SELECT
        product_id,
        COUNT(DISTINCT order_id)                    AS total_orders,
        SUM(quantity)                               AS total_units_sold,
        ROUND(SUM(net_revenue), 2)                  AS total_revenue,
        ROUND(SUM(gross_profit), 2)                 AS total_gross_profit,
        ROUND(AVG(discount_pct) * 100, 2)           AS avg_discount_pct,
        MAX(order_date)                             AS last_sold_date
    FROM {{ ref('fct_orders') }}
    WHERE order_status = 'DELIVERED'
    GROUP BY 1
),

inventory_stats AS (
    SELECT
        product_id,
        SUM(quantity_on_hand)                       AS total_stock,
        MAX(needs_reorder)                          AS needs_reorder,
        MIN(stock_status)                           AS stock_status
    FROM {{ ref('stg_inventory') }}
    GROUP BY 1
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.unit_cost,
    p.list_price,
    p.gross_margin,
    p.margin_pct,
    p.supplier_id,
    p.is_active,
    p.product_launch_date,

    -- Sales performance
    COALESCE(s.total_orders, 0)                     AS total_orders,
    COALESCE(s.total_units_sold, 0)                 AS total_units_sold,
    COALESCE(s.total_revenue, 0)                    AS total_revenue,
    COALESCE(s.total_gross_profit, 0)               AS total_gross_profit,
    COALESCE(s.avg_discount_pct, 0)                 AS avg_discount_pct,
    s.last_sold_date,

    -- Inventory
    COALESCE(i.total_stock, 0)                      AS total_stock,
    COALESCE(i.needs_reorder, FALSE)                AS needs_reorder,
    COALESCE(i.stock_status, 'UNKNOWN')             AS stock_status,

    -- Performance tier
    CASE
        WHEN COALESCE(s.total_revenue, 0) >= 50000  THEN 'TOP_SELLER'
        WHEN COALESCE(s.total_revenue, 0) >= 20000  THEN 'MID_SELLER'
        WHEN COALESCE(s.total_revenue, 0) > 0       THEN 'LOW_SELLER'
        ELSE                                             'NO_SALES'
    END                                             AS sales_tier

FROM       products        p
LEFT JOIN  sales_stats     s USING (product_id)
LEFT JOIN  inventory_stats i USING (product_id)