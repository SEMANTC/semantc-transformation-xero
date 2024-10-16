{{ config(
    tags=['transformed', 'xero', 'tracking_categories']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.tracking_category_id') AS tracking_category_id,
    JSON_VALUE(payload, '$.name') AS name,
    JSON_VALUE(payload, '$.status') AS status,
    ingestion_time
FROM 
    {{ source('raw', 'xero_tracking_categories') }}