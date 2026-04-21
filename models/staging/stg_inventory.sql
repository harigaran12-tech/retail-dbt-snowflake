WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_INVENTORY') }}
),

cleaned AS (
    SELECT
        INVENTORY_ID                                      AS inventory_id,
        PRODUCT_ID                                        AS product_id,
        WAREHOUSE_ID                                      AS warehouse_id,
        QUANTITY_ON_HAND                                  AS quantity_on_hand,
        REORDER_LEVEL                                     AS reorder_level,
        LAST_UPDATED                                      AS last_updated,

        -- Flag products that need reordering
        CASE
            WHEN QUANTITY_ON_HAND <= REORDER_LEVEL THEN TRUE
            ELSE FALSE
        END                                               AS needs_reorder,

        -- Stock health label
        CASE
            WHEN QUANTITY_ON_HAND = 0                THEN 'OUT_OF_STOCK'
            WHEN QUANTITY_ON_HAND <= REORDER_LEVEL   THEN 'LOW_STOCK'
            WHEN QUANTITY_ON_HAND <= REORDER_LEVEL 
                 * 2                                 THEN 'MEDIUM_STOCK'
            ELSE                                          'HEALTHY'
        END                                               AS stock_status

    FROM source
    WHERE INVENTORY_ID IS NOT NULL
)

SELECT * FROM cleaned