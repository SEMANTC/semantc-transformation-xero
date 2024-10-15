{{ config(
    tags=['transformed', 'xero', 'journals']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.journal_id') AS journal_id,
    SAFE_CAST(JSON_VALUE(payload, '$.created_date_utc') AS TIMESTAMP) AS created_date_utc,
    SAFE_CAST(JSON_VALUE(payload, '$.journal_date') AS DATE) AS journal_date,
    SAFE_CAST(JSON_VALUE(payload, '$.journal_number') AS INT64) AS journal_number,
    JSON_VALUE(payload, '$.reference') AS reference,
    JSON_VALUE(payload, '$.source_id') AS source_id,
    JSON_VALUE(payload, '$.source_type') AS source_type,
    ingestion_time
FROM 
    {{ source('raw', 'xero_journals') }}