{{ config(
    tags=['transformed', 'xero', 'manual_journals', 'lines']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.manual_journal_id') AS manual_journal_id,
    JSON_VALUE(manual_journal_line, '$.account_code') AS account_code,
    JSON_VALUE(manual_journal_line, '$.account_id') AS account_id,
    JSON_VALUE(manual_journal_line, '$.description') AS description,
    SAFE_CAST(JSON_VALUE(manual_journal_line, '$.is_blank') AS BOOL) AS is_blank,
    JSON_VALUE(manual_journal_line, '$.line_amount') AS line_amount,
    JSON_VALUE(manual_journal_line, '$.tax_amount') AS tax_amount,
    JSON_VALUE(manual_journal_line, '$.tax_type') AS tax_type,
    JSON_VALUE(manual_journal_line, '$.tracking') AS tracking,
    ingestion_time
FROM 
    {{ source('raw', 'xero_manual_journals') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.journal_lines')) AS manual_journal_line
WHERE 
    JSON_VALUE(manual_journal_line, '$.journal_line_id') IS NOT NULL