{{ config(
    tags=['transformed', 'xero', 'organisations', 'external_links']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.organisation_id') AS organisation_id,
    JSON_VALUE(link, '$.link_type') AS link_type,
    JSON_VALUE(link, '$.url') AS url,
    ingestion_time
FROM 
    {{ source('raw', 'xero_organisations') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.external_links')) AS link
WHERE 
    JSON_VALUE(link, '$.link_type') IS NOT NULL