{{ config(
    tags=['transformed', 'xero', 'batch_payments', 'validation_errors']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.batch_payment_id') AS batch_payment_id,
    JSON_VALUE(validation_error, '$.error_id') AS validation_error_id,
    JSON_VALUE(validation_error, '$.message') AS message,
    JSON_VALUE(validation_error, '$.field') AS field,
    CURRENT_TIMESTAMP() AS ingestion_time
FROM 
    {{ source('raw', 'xero_batch_payments') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.validation_errors')) AS validation_error
WHERE 
    JSON_VALUE(validation_error, '$.error_id') IS NOT NULL