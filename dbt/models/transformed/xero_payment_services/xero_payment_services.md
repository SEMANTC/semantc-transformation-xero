{{ config(
    tags=['transformed', 'xero', 'payment_services']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.payment_service_id') AS payment_service_id,
    JSON_VALUE(payload, '$.pay_now_text') AS pay_now_text,
    JSON_VALUE(payload, '$.payment_service_name') AS payment_service_name,
    JSON_VALUE(payload, '$.payment_service_type') AS payment_service_type,
    JSON_VALUE(payload, '$.payment_service_url') AS payment_service_url,
    -- JSON_VALUE(payload, '$.validation_errors') AS validation_errors,
    ingestion_time
FROM 
    {{ source('raw', 'xero_payment_services') }}