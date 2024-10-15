{{ config(
    tags=['transformed', 'xero', 'manual_journals']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.manual_journal_id') AS manual_journal_id,
    SAFE_CAST(JSON_VALUE(payload, '$.has_attachments') AS BOOL) AS has_attachments,
    SAFE_CAST(JSON_VALUE(payload, '$.date') AS DATE) AS date,
    SAFE_CAST(JSON_VALUE(payload, '$.journal_number') AS INT64) AS journal_number,
    JSON_VALUE(payload, '$.line_amount_types') AS line_amount_types,
    JSON_VALUE(payload, '$.narration') AS narration,
    JSON_VALUE(payload, '$.reference') AS reference,
    SAFE_CAST(JSON_VALUE(payload, '$.show_on_cash_basis_reports') AS BOOL) AS show_on_cash_basis_reports,
    JSON_VALUE(payload, '$.source_id') AS source_id,
    JSON_VALUE(payload, '$.source_type') AS source_type,
    JSON_VALUE(payload, '$.status') AS status,
    JSON_VALUE(payload, '$.status_attribute_string') AS status_attribute_string,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    JSON_VALUE(payload, '$.url') AS url,
    JSON_VALUE(payload, '$.validation_errors') AS validation_errors,
    JSON_VALUE(payload, '$.warnings') AS warnings,
    ingestion_time
FROM 
    {{ source('raw', 'xero_manual_journals') }}