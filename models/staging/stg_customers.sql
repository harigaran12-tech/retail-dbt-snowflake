WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_CUSTOMERS') }}
),
cleaned AS (
    SELECT
        CUSTOMER_ID                          AS customer_id,
        INITCAP(FIRST_NAME)                  AS first_name,
        INITCAP(LAST_NAME)                   AS last_name,
        INITCAP(FIRST_NAME) || ' ' || INITCAP(LAST_NAME)   AS full_name,
        LOWER(EMAIL)                         AS email,
        PHONE                                AS phone,
        CITY                                 AS city,
        STATE                                AS state,
        COUNTRY                              AS country,
        UPPER(SEGMENT)                       AS customer_segment,
        CREATED_AT                           AS customer_since
    FROM source
    WHERE CUSTOMER_ID IS NOT NULL
)
SELECT * FROM cleaned
