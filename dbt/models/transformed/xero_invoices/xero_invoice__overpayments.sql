{{ config(
    tags=['transformed', 'xero', 'invoices', 'overpayments']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.invoice_id') AS invoice_id,
    JSON_VALUE(overpayment, '$.overpayment_id') AS overpayment_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_invoices') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.overpayments')) AS overpayment
WHERE 
    JSON_VALUE(overpayment, '$.overpayment_id') IS NOT NULL