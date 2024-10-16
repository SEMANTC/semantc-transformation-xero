{{ config(
    tags=['transformed', 'xero', 'overpayments', 'payments']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.overpayment_id') AS overpayment_id,
    JSON_VALUE(payment, '$.payment_id') AS payment_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_overpayments') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.payments')) AS payment
WHERE 
    JSON_VALUE(payment, '$.payment_id') IS NOT NULL