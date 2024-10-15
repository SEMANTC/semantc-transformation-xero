{{ config(
    tags=['transformed', 'xero', 'credit_notes', 'allocations']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.credit_note_id') AS credit_note_id,
    JSON_VALUE(allocation, '$.allocation_id') AS allocation_id,
    SAFE_CAST(JSON_VALUE(allocation, '$.amount') AS FLOAT64) AS amount,
    SAFE_CAST(JSON_VALUE(allocation, '$.date') AS DATE) AS date,
    JSON_VALUE(allocation, '$.invoice.invoice_id') AS invoice_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_credit_notes') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.allocations')) AS allocation
WHERE
    JSON_VALUE(allocation, '$.allocation_id') IS NOT NULL