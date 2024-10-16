{{ config(
    tags=['transformed', 'xero', 'overpayments', 'allocations']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.prepayment_id') AS prepayment_id,
    JSON_VALUE(allocation, '$.allocation_id') AS allocation_id,
    SAFE_CAST(JSON_VALUE(allocation, '$.amount') AS FLOAT64) AS amount,
    SAFE_CAST(JSON_VALUE(allocation, '$.date') AS DATE) AS date,
    JSON_VALUE(allocation, '$.invoice.invoice_id') AS invoice_id,
    JSON_VALUE(allocation, '$.invoice.invoice_number') AS invoice_number,
    ingestion_time
FROM 
    {{ source('raw', 'xero_prepayments') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.allocations')) AS allocation
WHERE 
    JSON_VALUE(allocation, '$.allocation_id') IS NOT NULL