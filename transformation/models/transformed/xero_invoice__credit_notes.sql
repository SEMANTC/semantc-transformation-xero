{{ config(
    tags=['transformed', 'xero', 'invoices', 'invoice__credit_notes']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.invoice_id') AS invoice_id,
    JSON_VALUE(credit_note, '$.credit_note_id') AS credit_note_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_invoices') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.credit_notes')) AS credit_note
WHERE JSON_VALUE(credit_note, '$.credit_note_id') IS NOT NULL