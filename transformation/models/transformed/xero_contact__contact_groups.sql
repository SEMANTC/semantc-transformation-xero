{{ config(
    tags=['transformed', 'xero', 'contacts', 'contact__group_contacts']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.contact_id') AS contact_id,
    JSON_VALUE(contact_group, '$.name') AS group_name,
    ingestion_time
FROM 
    {{ source('raw', 'xero_contacts') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.contact_groups')) AS contact_group
WHERE JSON_VALUE(contact_group, '$.name') IS NOT NULL