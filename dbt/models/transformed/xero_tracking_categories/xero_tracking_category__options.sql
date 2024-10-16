{{ config(
    tags=['transformed', 'xero', 'tracking_categories', 'options']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.tracking_category_id') AS tracking_category_id,
    JSON_VALUE(option, '$.tracking_option_id') AS tracking_option_id,
    JSON_VALUE(option, '$.name') AS name,
    JSON_VALUE(option, '$.status') AS status,
    ingestion_time
FROM 
    {{ source('raw', 'xero_tracking_categories') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.options')) AS option
WHERE 
    JSON_VALUE(option, '$.tracking_option_id') IS NOT NULL