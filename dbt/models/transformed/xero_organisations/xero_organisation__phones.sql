{{ config(
    tags=['transformed', 'xero', 'organisations', 'phones']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.organisation_id') AS organisation_id,
    JSON_VALUE(phone, '$.phone_type') AS phone_type,
    JSON_VALUE(phone, '$.phone_country_code') AS phone_country_code,
    JSON_VALUE(phone, '$.phone_area_code') AS phone_area_code,
    JSON_VALUE(phone, '$.phone_number') AS phone_number,
    ingestion_time
FROM 
    {{ source('raw', 'xero_organisations') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.phones')) AS phone
WHERE 
    JSON_VALUE(phone, '$.phone_type') IS NOT NULL