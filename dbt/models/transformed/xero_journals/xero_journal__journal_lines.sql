{{ config(
    tags=['transformed', 'xero', 'journals', 'journal_lines']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.journal_id') AS journal_id,
    JSON_VALUE(journal_line, '$.journal_line_id') AS journal_line_id,
    JSON_VALUE(journal_line, '$.account_code') AS account_code,
    JSON_VALUE(journal_line, '$.account_id') AS account_id,
    JSON_VALUE(journal_line, '$.account_name') AS account_name,
    JSON_VALUE(journal_line, '$.account_type') AS account_type,
    JSON_VALUE(journal_line, '$.description') AS description,
    SAFE_CAST(JSON_VALUE(journal_line, '$.gross_amount') AS FLOAT64) AS gross_amount,
    SAFE_CAST(JSON_VALUE(journal_line, '$.net_amount') AS FLOAT64) AS net_amount,
    SAFE_CAST(JSON_VALUE(journal_line, '$.tax_amount') AS FLOAT64) AS tax_amount,
    JSON_VALUE(journal_line, '$.tax_name') AS tax_name,
    JSON_VALUE(journal_line, '$.tax_type') AS tax_type,
    ingestion_time
FROM 
    {{ source('raw', 'xero_journals') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.journal_lines')) AS journal_line
WHERE 
    JSON_VALUE(journal_line, '$.journal_line_id') IS NOT NULL