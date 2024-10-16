{{ config(
    tags=['transformed', 'xero', 'budgets', 'tracking']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.budget_id') AS budget_id,
    JSON_VALUE(tracking, '$.tracking_category_id') AS tracking_category_id,
    JSON_VALUE(tracking, '$.name') AS name,
    JSON_VALUE(tracking, '$.option') AS option,
    ingestion_time
FROM 
    {{ source('raw', 'xero_budgets') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.tracking')) AS tracking
WHERE 
    JSON_VALUE(tracking, '$.tracking_category_id') IS NOT NULL