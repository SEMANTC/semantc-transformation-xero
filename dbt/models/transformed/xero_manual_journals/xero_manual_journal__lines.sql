{{ config(
    tags=['transformed', 'xero', 'manual_journals', 'lines']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.manual_journal_id') AS manual_journal_id,
    JSON_VALUE(manual_journal_line, '$.journal_line_id') AS journal_line_id,
    JSON_VALUE(manual_journal_line, '$.account_id') AS account_id,
    JSON_VALUE(manual_journal_line, '$.account_code') AS account_code,
    JSON_VALUE(manual_journal_line, '$.account_name') AS account_name,
    JSON_VALUE(manual_journal_line, '$.account_type') AS account_type,
    JSON_VALUE(manual_journal_line, '$.description') AS description,
    SAFE_CAST(JSON_VALUE(manual_journal_line, '$.net_amount') AS FLOAT64) AS net_amount,
    SAFE_CAST(JSON_VALUE(manual_journal_line, '$.gross_amount') AS FLOAT64) AS gross_amount,
    SAFE_CAST(JSON_VALUE(manual_journal_line, '$.tax_amount') AS FLOAT64) AS tax_amount,
    JSON_VALUE(manual_journal_line, '$.tax_type') AS tax_type,
    JSON_VALUE(manual_journal_line, '$.tax_name') AS tax_name,
    SAFE_CAST(JSON_VALUE(manual_journal_line, '$.is_blank') AS BOOL) AS is_blank,
    SAFE_CAST(JSON_VALUE(manual_journal_line, '$.created_date_utc') AS TIMESTAMP) AS journal_line_created_date_utc,
    SAFE_CAST(JSON_VALUE(manual_journal_line, '$.updated_date_utc') AS TIMESTAMP) AS journal_line_updated_date_utc,
    ingestion_time
FROM 
    {{ source('raw', 'xero_manual_journals') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.journal_lines')) AS manual_journal_line
WHERE 
    JSON_VALUE(manual_journal_line, '$.journal_line_id') IS NOT NULL