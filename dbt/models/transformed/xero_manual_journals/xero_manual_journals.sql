{{ config(
    tags=['transformed', 'xero', 'manual_journals']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.manual_journal_id') AS manual_journal_id,
    SAFE_CAST(JSON_VALUE(payload, '$.journal_number') AS INT64) AS journal_number,
    SAFE_CAST(JSON_VALUE(payload, '$.journal_date') AS DATE) AS journal_date,
    JSON_VALUE(payload, '$.narration') AS narration,
    JSON_VALUE(payload, '$.reference') AS reference,
    JSON_VALUE(payload, '$.source_id') AS source_id,
    JSON_VALUE(payload, '$.source_type') AS source_type,
    SAFE_CAST(JSON_VALUE(payload, '$.created_date_utc') AS TIMESTAMP) AS created_date_utc,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    JSON_VALUE(payload, '$.line_amount_types') AS line_amount_types,
    SAFE_CAST(JSON_VALUE(payload, '$.show_on_cash_basis_reports') AS BOOL) AS show_on_cash_basis_reports,
    SAFE_CAST(JSON_VALUE(payload, '$.has_attachments') AS BOOL) AS has_attachments,
    JSON_VALUE(payload, '$.status') AS status,
    JSON_VALUE(payload, '$.status_attribute_string') AS status_attribute_string,
    JSON_VALUE(payload, '$.url') AS url,
    JSON_VALUE(payload, '$.validation_errors') AS validation_errors,
    JSON_VALUE(payload, '$.warnings') AS warnings,
    ingestion_time,
FROM 
    {{ source('raw', 'xero_manual_journals') }}
WHERE
    JSON_VALUE(payload, '$.manual_journal_id') IS NOT NULL