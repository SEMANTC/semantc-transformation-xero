{{ config(
    tags=['transformed', 'xero', 'manual_journals', 'lines', 'tracking']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.manual_journal_id') AS manual_journal_id,
    JSON_VALUE(line, '$.line_item_id') AS line_item_id,
    JSON_VALUE(tracking, '$.tracking_category_id') AS tracking_category_id,
    JSON_VALUE(tracking, '$.name') AS tracking_category_name,
    JSON_VALUE(tracking, '$.option') AS tracking_option,
    ingestion_time
FROM 
    {{ source('raw', 'xero_manual_journals') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.journal_lines')) AS line,
    UNNEST(JSON_QUERY_ARRAY(line, '$.tracking')) AS tracking
WHERE 
    JSON_VALUE(line, '$.line_item_id') IS NOT NULL
    AND JSON_VALUE(tracking, '$.tracking_category_id') IS NOT NULL