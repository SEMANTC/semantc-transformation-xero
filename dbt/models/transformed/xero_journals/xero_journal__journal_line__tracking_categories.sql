{{ config(
    tags=['transformed', 'xero', 'journals', 'tracking_categories']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.journal_id') AS journal_id,
    JSON_VALUE(payload, '$.journal_lines.journal_line_id') AS journal_line_id,
    JSON_VALUE(tracking_category, '$.tracking_category_id') AS tracking_category_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_journals') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.journal_lines.tracking_categories')) AS tracking_category
WHERE 
    JSON_VALUE(tracking_category, '$.tracking_category_id') IS NOT NULL