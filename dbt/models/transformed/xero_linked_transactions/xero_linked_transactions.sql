{{ config(
    tags=['transformed', 'xero', 'linked_transactions']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.linked_transaction_id') AS linked_transaction_id,
    JSON_VALUE(payload, '$.contact_id') AS contact_id,
    JSON_VALUE(payload, '$.source_line_item_id') AS source_line_item_id,
    JSON_VALUE(payload, '$.source_transaction_id') AS source_transaction_id,
    JSON_VALUE(payload, '$.source_transaction_type_code') AS source_transaction_type_code,
    JSON_VALUE(payload, '$.status') AS status,
    JSON_VALUE(payload, '$.target_line_item_id') AS target_line_item_id,
    JSON_VALUE(payload, '$.target_transaction_id') AS target_transaction_id,
    JSON_VALUE(payload, '$.type') AS type,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    JSON_VALUE(payload, '$.validation_errors') AS validation_errors,
    ingestion_time
FROM 
    {{ source('raw', 'xero_linked_transactions') }}