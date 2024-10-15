{{ config(
    tags=['transformed', 'xero', 'employees']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.EmployeeID') AS employee_id,
    JSON_VALUE(payload, '$.FirstName') AS first_name,
    JSON_VALUE(payload, '$.LastName') AS last_name,
    JSON_VALUE(payload, '$.Status') AS status,
    JSON_VALUE(payload, '$.ExternalLink.Url') AS external_link_url,
    JSON_VALUE(payload, '$.ExternalLink.Description') AS external_link_description,
    SAFE_CAST(JSON_VALUE(payload, '$.UpdatedDateUTC') AS TIMESTAMP) AS updated_date_utc,
    ingestion_time
FROM 
    {{ source('raw', 'xero_employees') }}