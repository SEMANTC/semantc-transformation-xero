{{ config(
    tags=['transformed', 'xero', 'organisations', 'external_links']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.organisation_id') AS organisation_id,
    JSON_VALUE(external_link, '$.service_name') AS service_name,
    JSON_VALUE(external_link, '$.url') AS url,
    ingestion_time
FROM 
    {{ source('raw', 'xero_organisations') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.external_links')) AS external_link
WHERE 
    JSON_VALUE(external_link, '$.service_name') IS NOT NULL