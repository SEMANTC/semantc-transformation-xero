{{ config(
    tags=['transformed', 'xero', 'invoices', 'prepayments']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.invoice_id') AS invoice_id,
    JSON_VALUE(prepayment, '$.prepayment_id') AS prepayment_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_invoices') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.prepayments')) AS prepayment
WHERE 
    JSON_VALUE(prepayment, '$.prepayment_id') IS NOT NULL