{{ config(
    tags=['transformed', 'xero', 'bank_transfers', 'validation_errors'],
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.bank_transfer_id') AS bank_transfer_id,
    JSON_VALUE(validation_error, '$.error_id') AS validation_error_id,
    JSON_VALUE(validation_error, '$.message') AS message,
    JSON_VALUE(validation_error, '$.field') AS field,
    ingestion_time
FROM 
    {{ source('raw', 'xero_bank_transfers') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.validation_errors')) AS validation_error
WHERE 
    JSON_VALUE(validation_error, '$.error_id') IS NOT NULL