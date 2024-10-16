{{ config(
    tags=['transformed', 'xero', 'budgets', 'budget_lines']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.budget_id') AS budget_id,
    JSON_VALUE(budget_line, '$.account_id') AS account_id,
    JSON_VALUE(budget_line, '$.account_code') AS account_code,
    ingestion_time
FROM 
    {{ source('raw', 'xero_budgets') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.budget_lines')) AS budget_line
WHERE 
    JSON_VALUE(budget_line, '$.account_id') IS NOT NULL