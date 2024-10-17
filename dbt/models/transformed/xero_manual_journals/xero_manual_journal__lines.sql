{{ config(
    tags=['transformed', 'xero', 'manual_journals', 'lines']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.manual_journal_id') AS manual_journal_id,
    JSON_VALUE(line, '$.line_item_id') AS line_item_id,
    JSON_VALUE(line, '$.account_code') AS account_code,
    JSON_VALUE(line, '$.account_id') AS account_id,
    JSON_VALUE(line, '$.description') AS description,
    SAFE_CAST(JSON_VALUE(line, '$.is_blank') AS BOOL) AS is_blank,
    SAFE_CAST(JSON_VALUE(line, '$.line_amount') AS FLOAT64) AS line_amount,
    SAFE_CAST(JSON_VALUE(line, '$.tax_amount') AS FLOAT64) AS tax_amount,
    JSON_VALUE(line, '$.tax_type') AS tax_type,
    ingestion_time
FROM 
    {{ source('raw', 'xero_manual_journals') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.journal_lines')) AS line
WHERE 
    JSON_VALUE(line, '$.line_item_id') IS NOT NULL