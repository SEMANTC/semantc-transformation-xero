{{ config(
    tags=['transformed', 'xero', 'contacts', 'contact__phones']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.contact_id') AS contact_id,
    JSON_VALUE(phone, '$.phone_area_code') AS phone_area_code,
    JSON_VALUE(phone, '$.phone_country_code') AS phone_country_code,
    JSON_VALUE(phone, '$.phone_number') AS phone_number,
    JSON_VALUE(phone, '$.phone_type') AS phone_type,
    ingestion_time
FROM 
    {{ source('raw', 'xero_contacts') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.phones')) AS phone
WHERE JSON_VALUE(phone, '$.phone_type') IS NOT NULL