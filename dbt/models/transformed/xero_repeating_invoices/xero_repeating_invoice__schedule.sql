{{ config(
    tags=['transformed', 'xero', 'repeating_invoices', 'schedule']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.repeating_invoice_id') AS repeating_invoice_id,
    SAFE_CAST(JSON_VALUE(payload, '$.schedule.due_date') AS INT64) AS due_date,
    JSON_VALUE(payload, '$.schedule.due_date_type') AS due_date_type,
    SAFE_CAST(JSON_VALUE(payload, '$.schedule.end_date') AS DATE) AS end_date,
    SAFE_CAST(JSON_VALUE(payload, '$.schedule.next_scheduled_date') AS DATE) AS next_scheduled_date,
    SAFE_CAST(JSON_VALUE(payload, '$.schedule.period') AS INT64) AS period,
    SAFE_CAST(JSON_VALUE(payload, '$.schedule.start_date') AS DATE) AS start_date,
    JSON_VALUE(payload, '$.schedule.unit') AS unit,
    ingestion_time
FROM 
    {{ source('raw', 'xero_repeating_invoices') }}