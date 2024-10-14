{{ config(
    tags=['transformed', 'xero', 'invoices', 'attachments']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.invoice_id') AS invoice_id,
    JSON_VALUE(attachment, '$.attachment_id') AS attachment_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_invoices') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.attachments')) AS attachment
WHERE 
    JSON_VALUE(attachment, '$.attachment_id') IS NOT NULL