{{ config(
    tags=['transformed', 'xero', 'tax_rates']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.name') AS name,
    JSON_VALUE(payload, '$.tax_type') AS tax_type,
    JSON_VALUE(payload, '$.status') AS status,
    JSON_VALUE(payload, '$.report_tax_type') AS report_tax_type,
    SAFE_CAST(JSON_VALUE(payload, '$.can_apply_to_assets') AS BOOL) AS can_apply_to_assets,
    SAFE_CAST(JSON_VALUE(payload, '$.can_apply_to_equity') AS BOOL) AS can_apply_to_equity,
    SAFE_CAST(JSON_VALUE(payload, '$.can_apply_to_expenses') AS BOOL) AS can_apply_to_expenses,
    SAFE_CAST(JSON_VALUE(payload, '$.can_apply_to_liabilities') AS BOOL) AS can_apply_to_liabilities,
    SAFE_CAST(JSON_VALUE(payload, '$.can_apply_to_revenue') AS BOOL) AS can_apply_to_revenue,
    SAFE_CAST(JSON_VALUE(payload, '$.display_tax_rate') AS FLOAT64) AS display_tax_rate,
    SAFE_CAST(JSON_VALUE(payload, '$.effective_rate') AS FLOAT64) AS effective_rate,
    SAFE_CAST(JSON_VALUE(payload, '$.ingestion_time') AS TIMESTAMP) AS ingestion_time
FROM 
    {{ source('raw', 'xero_tax_rates') }}