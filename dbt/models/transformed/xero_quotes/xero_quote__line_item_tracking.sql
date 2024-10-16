{{ config(
    tags=['transformed', 'xero', 'quotes', 'line_items', 'tracking']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.quote_id') AS quote_id,
    JSON_VALUE(line_item, '$.line_item_id') AS line_item_id,
    JSON_VALUE(tracking, '$.tracking_category_id') AS tracking_category_id,
    JSON_VALUE(tracking, '$.tracking_option_id') AS tracking_option_id,
    JSON_VALUE(tracking, '$.name') AS tracking_name,
    JSON_VALUE(tracking, '$.option') AS tracking_option,
    ingestion_time
FROM 
    {{ source('raw', 'xero_quotes') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.line_items')) AS line_item,
    UNNEST(JSON_QUERY_ARRAY(line_item, '$.tracking')) AS tracking
WHERE 
    JSON_VALUE(line_item, '$.line_item_id') IS NOT NULL
    AND JSON_VALUE(tracking, '$.tracking_category_id') IS NOT NULL