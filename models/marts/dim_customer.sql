{{
    config(materialized = 'table')
}}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

order_stats AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id)                    AS total_orders,
        SUM(net_revenue)                            AS lifetime_revenue,
        AVG(net_revenue)                            AS avg_order_value,
        MIN(order_date)                             AS first_order_date,
        MAX(order_date)                             AS last_order_date,
        SUM(CASE WHEN order_status = 'RETURNED'
                 THEN 1 ELSE 0 END)                 AS total_returns,
        SUM(CASE WHEN order_status = 'CANCELLED'
                 THEN 1 ELSE 0 END)                 AS total_cancellations
    FROM {{ ref('stg_orders') }}
    GROUP BY 1
)

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.full_name,
    c.email,
    c.phone,
    c.city,
    c.state,
    c.country,
    c.customer_segment,
    c.customer_since,

    -- Order behaviour
    COALESCE(o.total_orders, 0)                     AS total_orders,
    COALESCE(o.lifetime_revenue, 0)                 AS lifetime_revenue,
    COALESCE(o.avg_order_value, 0)                  AS avg_order_value,
    o.first_order_date,
    o.last_order_date,
    COALESCE(o.total_returns, 0)                    AS total_returns,
    COALESCE(o.total_cancellations, 0)              AS total_cancellations,
    DATEDIFF('day',
             o.last_order_date,
             CURRENT_DATE())                        AS days_since_last_order,

    -- Customer value tier
    CASE
        WHEN COALESCE(o.lifetime_revenue, 0) >= 10000 THEN 'HIGH_VALUE'
        WHEN COALESCE(o.lifetime_revenue, 0) >= 3000  THEN 'MID_VALUE'
        WHEN COALESCE(o.lifetime_revenue, 0) > 0      THEN 'LOW_VALUE'
        ELSE                                               'NO_ORDERS'
    END                                             AS customer_value_tier

FROM      customers  c
LEFT JOIN order_stats o USING (customer_id)