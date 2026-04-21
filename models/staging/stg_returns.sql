WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_RETURNS') }}
),

cleaned AS (
    SELECT
        RETURN_ID                                         AS return_id,
        ORDER_ID                                          AS order_id,
        RETURN_DATE                                       AS return_date,
        UPPER(TRIM(REASON))                               AS return_reason,
        REFUND_AMOUNT                                     AS refund_amount,
        UPPER(TRIM(RETURN_STATUS))                        AS return_status,

        -- Flag approved refunds
        CASE
            WHEN UPPER(TRIM(RETURN_STATUS)) = 'APPROVED' THEN TRUE
            ELSE FALSE
        END                                               AS is_refund_approved,

        -- Return processing time (days since return was raised)
        DATEDIFF('day', RETURN_DATE, CURRENT_DATE())      AS days_since_return

    FROM source
    WHERE RETURN_ID IS NOT NULL
)

SELECT * FROM cleaned