{{ config(
    tags=['transformed', 'xero', 'organisations', 'addresses']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.organisation_id') AS organisation_id,
    JSON_VALUE(address, '$.address_type') AS address_type,
    JSON_VALUE(address, '$.address_line1') AS address_line1,
    JSON_VALUE(address, '$.address_line2') AS address_line2,
    JSON_VALUE(address, '$.address_line3') AS address_line3,
    JSON_VALUE(address, '$.address_line4') AS address_line4,
    JSON_VALUE(address, '$.attention_to') AS attention_to,
    JSON_VALUE(address, '$.city') AS city,
    JSON_VALUE(address, '$.country') AS country,
    JSON_VALUE(address, '$.postal_code') AS postal_code,
    JSON_VALUE(address, '$.region') AS region,
    ingestion_time
FROM 
    {{ source('raw', 'xero_organisations') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.addresses')) AS address
WHERE 
    JSON_VALUE(address, '$.address_type') IS NOT NULL