{{ config(
    tags=['transformed', 'xero', 'tax_rates', 'tax_components']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.name') AS tax_rate_name,
    JSON_VALUE(component, '$.name') AS component_name,
    SAFE_CAST(JSON_VALUE(component, '$.rate') AS FLOAT64) AS component_rate,
    SAFE_CAST(JSON_VALUE(component, '$.is_compound') AS BOOL) AS is_compound,
    SAFE_CAST(JSON_VALUE(component, '$.is_non_recoverable') AS BOOL) AS is_non_recoverable,
    SAFE_CAST(JSON_VALUE(payload, '$.ingestion_time') AS TIMESTAMP) AS ingestion_time
FROM 
    {{ source('raw', 'xero_tax_rates') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.tax_components')) AS component
WHERE 
    JSON_VALUE(component, '$.name') IS NOT NULL