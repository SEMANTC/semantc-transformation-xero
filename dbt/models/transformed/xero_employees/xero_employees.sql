{{ config(
    tags=['transformed', 'xero', 'employees']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.employee_id') AS employee_id,
    JSON_VALUE(payload, '$.external_link.url') AS external_link_url,
    JSON_VALUE(payload, '$.external_link.description') AS external_link_description,
    JSON_VALUE(payload, '$.first_name') AS first_name,
    JSON_VALUE(payload, '$.last_name') AS last_name,
    JSON_VALUE(payload, '$.status') AS status,
    JSON_VALUE(payload, '$.status_attribute_string') AS status_attribute_string,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    JSON_VALUE(payload, '$.validation_errors') AS validation_errors,
    ingestion_time
FROM 
    {{ source('raw', 'xero_employees') }}