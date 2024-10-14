{{ config(
    tags=['transformed', 'xero', 'budgets', 'budget_lines']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.budget_id') AS budget_id,
    JSON_VALUE(budget_line, '$.budget_line_id') AS budget_line_id,
    JSON_VALUE(budget_line, '$.description') AS budget_line_description,
    SAFE_CAST(JSON_VALUE(budget_line, '$.amount') AS FLOAT64) AS budget_line_amount,
    SAFE_CAST(JSON_VALUE(budget_line, '$.created_date_utc') AS TIMESTAMP) AS budget_line_created_date_utc,
    SAFE_CAST(JSON_VALUE(budget_line, '$.updated_date_utc') AS TIMESTAMP) AS budget_line_updated_date_utc,
    ingestion_time
FROM 
    {{ source('raw', 'xero_budgets') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.budget_lines')) AS budget_line
WHERE 
    JSON_VALUE(budget_line, '$.budget_line_id') IS NOT NULL