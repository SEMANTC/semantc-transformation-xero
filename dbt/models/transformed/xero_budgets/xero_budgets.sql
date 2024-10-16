{{ config(
    tags=['transformed', 'xero', 'budgets']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.budget_id') AS budget_id,
    JSON_VALUE(payload, '$.description') AS description,
    JSON_VALUE(payload, '$.type') AS type,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    ingestion_time
FROM 
    {{ source('raw', 'xero_budgets') }}