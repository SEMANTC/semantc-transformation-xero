{{ config(
    tags=['transformed', 'xero', 'manual_journals', 'tracking_categories']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.manual_journal_id') AS manual_journal_id,
    JSON_VALUE(tracking_category, '$.tracking_category_id') AS tracking_category_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_manual_journals') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.journal_lines.tracking')) AS tracking_category
WHERE 
    JSON_VALUE(tracking_category, '$.tracking_category_id') IS NOT NULL