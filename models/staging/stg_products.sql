WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_PRODUCTS') }}
),

cleaned AS (
    SELECT
        PRODUCT_ID                                      AS product_id,
        TRIM(PRODUCT_NAME)                              AS product_name,
        UPPER(TRIM(CATEGORY))                           AS category,
        UPPER(TRIM(SUBCATEGORY))                        AS subcategory,
        UNIT_COST                                       AS unit_cost,
        LIST_PRICE                                      AS list_price,
        ROUND(LIST_PRICE - UNIT_COST, 2)                AS gross_margin,
        ROUND((LIST_PRICE - UNIT_COST) 
              / NULLIF(LIST_PRICE, 0) * 100, 2)         AS margin_pct,
        SUPPLIER_ID                                     AS supplier_id,
        IS_ACTIVE                                       AS is_active,
        CREATED_AT                                      AS product_launch_date
    FROM source
    WHERE PRODUCT_ID IS NOT NULL
)

SELECT * FROM cleaned