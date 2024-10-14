{{ config(
    tags=['transformed', 'xero', 'currencies']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.code') AS code,
    JSON_VALUE(payload, '$.description') AS description,
    ingestion_time
FROM 
    {{ source('raw', 'xero_currencies') }}