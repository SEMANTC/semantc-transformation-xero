{{ config(
    tags=['transformed', 'xero', 'credit_notes', 'payments']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.credit_note_id') AS credit_note_id,
    JSON_VALUE(payment, '$.payment_id') AS payment_id,
    SAFE_CAST(JSON_VALUE(payment, '$.amount') AS FLOAT64) AS amount,
    SAFE_CAST(JSON_VALUE(payment, '$.date') AS DATE) AS date,
    ingestion_time
FROM 
    {{ source('raw', 'xero_credit_notes') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.payments')) AS payment
WHERE 
    JSON_VALUE(payment, '$.payment_id') IS NOT NULL