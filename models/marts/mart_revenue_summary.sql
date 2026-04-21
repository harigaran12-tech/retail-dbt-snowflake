{{
    config(materialized = 'table')
}}

SELECT
    order_month,
    order_quarter,
    order_year,
    region,
    channel,
    product_category,
    customer_segment,

    -- Volume
    COUNT(DISTINCT order_id)                        AS total_orders,
    COUNT(DISTINCT customer_id)                     AS unique_customers,
    SUM(quantity)                                   AS total_units_sold,

    -- Revenue
    ROUND(SUM(net_revenue), 2)                      AS total_revenue,
    ROUND(AVG(net_revenue), 2)                      AS avg_order_value,

    -- Profitability
    ROUND(SUM(gross_profit), 2)                     AS total_gross_profit,
    ROUND(SUM(gross_profit)
          / NULLIF(SUM(net_revenue), 0) * 100, 2)   AS gp_margin_pct,

    -- Discounting
    ROUND(AVG(discount_pct) * 100, 2)               AS avg_discount_pct,

    -- Returns (from order status)
    SUM(CASE WHEN order_status = 'RETURNED'
             THEN 1 ELSE 0 END)                     AS total_returns,
    ROUND(SUM(CASE WHEN order_status = 'RETURNED'
                   THEN 1 ELSE 0 END)
          / NULLIF(COUNT(order_id), 0) * 100, 2)    AS return_rate_pct

FROM {{ ref('fct_orders') }}
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1 DESC, total_revenue DESC