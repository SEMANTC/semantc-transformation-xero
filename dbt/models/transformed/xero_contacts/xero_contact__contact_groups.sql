{{ config(
    tags=['transformed', 'xero', 'contacts', 'contact_groups']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.contact_id') AS contact_id,
    JSON_VALUE(group, '$.contact_group_id') AS contact_group_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_contacts') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.contact_groups')) AS group
WHERE 
    JSON_VALUE(group, '$.contact_group_id') IS NOT NULL