{{ config(
    tags=['transformed', 'xero', 'budgets', 'tracking_categories']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.budget_id') AS budget_id,
    JSON_VALUE(tracking, '$.tracking_category_id') AS tracking_category_id,
    JSON_VALUE(tracking, '$.name') AS tracking_category_name,
    JSON_VALUE(tracking, '$.status') AS tracking_category_status,
    SAFE_CAST(JSON_VALUE(tracking, '$.created_date_utc') AS TIMESTAMP) AS tracking_category_created_date_utc,
    SAFE_CAST(JSON_VALUE(tracking, '$.updated_date_utc') AS TIMESTAMP) AS tracking_category_updated_date_utc,
    SAFE_CAST(JSON_VALUE(tracking, '$.ingestion_time') AS TIMESTAMP) AS tracking_category_ingestion_time
FROM 
    {{ source('raw', 'xero_budgets') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.tracking')) AS tracking
WHERE 
    JSON_VALUE(tracking, '$.tracking_category_id') IS NOT NULL