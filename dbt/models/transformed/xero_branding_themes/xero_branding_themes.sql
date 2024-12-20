{{ config(
    tags=['transformed', 'xero', 'branding_themes']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.branding_theme_id') AS branding_theme_id,
    SAFE_CAST(JSON_VALUE(payload, '$.created_date_utc') AS TIMESTAMP) AS created_date_utc,
    JSON_VALUE(payload, '$.logo_url') AS logo_url,
    JSON_VALUE(payload, '$.name') AS name,
    SAFE_CAST(JSON_VALUE(payload, '$.sort_order') AS INT64) AS sort_order,
    JSON_VALUE(payload, '$.type') AS type,
    ingestion_time
FROM 
    {{ source('raw', 'xero_branding_themes') }}