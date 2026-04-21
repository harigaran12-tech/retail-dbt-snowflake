SELECT
    order_id,
    net_revenue
FROM {{ ref('fct_orders') }}
WHERE order_status = 'DELIVERED'
  AND net_revenue  < 0