{{ config(
    tags=['transformed', 'xero', 'invoices', 'payments']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.invoice_id') AS invoice_id,
    JSON_VALUE(payment, '$.payment_id') AS payment_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_invoices') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.payments')) AS payment
WHERE 
    JSON_VALUE(payment, '$.payment_id') IS NOT NULL