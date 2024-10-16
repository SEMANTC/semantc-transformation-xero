{{ config(
    tags=['transformed', 'xero', 'contacts', 'persons']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.contact_id') AS contact_id,
    JSON_VALUE(person, '$.email_address') AS email_address,
    JSON_VALUE(person, '$.first_name') AS first_name,
    SAFE_CAST(JSON_VALUE(person, '$.include_in_emails') AS BOOL) AS include_in_emails,
    JSON_VALUE(person, '$.last_name') AS last_name,
    ingestion_time
FROM 
    {{ source('raw', 'xero_contacts') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.contact_persons')) AS person
WHERE JSON_VALUE(person, '$.first_name') IS NOT NULL OR JSON_VALUE(person, '$.last_name') IS NOT NULL
