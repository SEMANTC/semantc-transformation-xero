{{ config(
    tags=['transformed', 'xero', 'contact_groups', 'contacts']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.contact_group_id') AS contact_group_id,
    JSON_VALUE(contact, '$.contact_id') AS contact_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_contact_groups') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.contacts')) AS contact
WHERE 
    JSON_VALUE(contact, '$.contact_id') IS NOT NULL