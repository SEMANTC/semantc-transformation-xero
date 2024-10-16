{{ config(
    tags=['transformed', 'xero', 'budgets', 'budget_balances']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.budget_id') AS budget_id,
    JSON_VALUE(budget_line, '$.account_id') AS account_id,
    JSON_VALUE(balance, '$.period') AS period,
    SAFE_CAST(JSON_VALUE(balance, '$.amount') AS FLOAT64) AS amount,
    ingestion_time
FROM 
    {{ source('raw', 'xero_budgets') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.budget_lines')) AS budget_line,
    UNNEST(JSON_QUERY_ARRAY(budget_line, '$.budget_balances')) AS balance
WHERE 
    JSON_VALUE(budget_line, '$.account_id') IS NOT NULL
    AND JSON_VALUE(balance, '$.period') IS NOT NULL