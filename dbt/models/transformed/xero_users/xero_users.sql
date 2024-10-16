{{ config(
    tags=['transformed', 'xero', 'users']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.user_id') AS user_id,
    JSON_VALUE(payload, '$.email_address') AS email_address,
    JSON_VALUE(payload, '$.first_name') AS first_name,
    SAFE_CAST(JSON_VALUE(payload, '$.is_subscriber') AS BOOL) AS is_subscriber,
    JSON_VALUE(payload, '$.last_name') AS last_name,
    JSON_VALUE(payload, '$.organisation_role') AS organisation_role,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    ingestion_time
FROM 
    {{ source('raw', 'xero_users') }}