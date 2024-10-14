{{ config(
    tags=['transformed', 'xero', 'contact_groups']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.contact_group_id') AS contact_group_id,
    JSON_VALUE(payload, '$.name') AS name,
    JSON_VALUE(payload, '$.status') AS status,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    ingestion_time
FROM 
    {{ source('raw', 'xero_contact_groups') }}