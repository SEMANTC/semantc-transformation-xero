{{ config(
    tags=['transformed', 'xero', 'prepayments', 'payments']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.prepayment_id') AS prepayment_id,
    JSON_VALUE(payment, '$.payment_id') AS payment_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_prepayments') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.payments')) AS payment
WHERE 
    JSON_VALUE(payment, '$.payment_id') IS NOT NULL