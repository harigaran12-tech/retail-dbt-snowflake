WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_ORDERS') }}
),

cleaned AS (
    SELECT
        ORDER_ID                                                    AS order_id,
        ORDER_DATE                                                  AS order_date,
        CUSTOMER_ID                                                 AS customer_id,
        PRODUCT_ID                                                  AS product_id,
        QUANTITY                                                    AS quantity,
        UNIT_PRICE                                                  AS unit_price,
        COALESCE(DISCOUNT_PCT, 0)                                   AS discount_pct,
        ROUND(QUANTITY * UNIT_PRICE * (1 - COALESCE(DISCOUNT_PCT,0)), 2) AS net_revenue,
        UPPER(TRIM(ORDER_STATUS))                                   AS order_status,
        UPPER(TRIM(CHANNEL))                                        AS channel,
        UPPER(TRIM(REGION))                                         AS region,
        CREATED_AT,
        UPDATED_AT
    FROM source
    WHERE ORDER_ID IS NOT NULL
)

SELECT * FROM cleaned
