{{ config(
    tags=['transformed', 'xero', 'batch_payments']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.batch_payment_id') AS batch_payment_id,
    JSON_VALUE(payload, '$.account.account_id') AS account_id,
    SAFE_CAST(JSON_VALUE(payload, '$.amount') AS FLOAT64) AS amount,
    JSON_VALUE(payload, '$.code') AS code,
    SAFE_CAST(JSON_VALUE(payload, '$.date') AS DATE) AS date,
    JSON_VALUE(payload, '$.date_string') AS date_string,
    JSON_VALUE(payload, '$.details') AS details,
    SAFE_CAST(JSON_VALUE(payload, '$.is_reconciled') AS BOOL) AS is_reconciled,
    JSON_VALUE(payload, '$.narrative') AS narrative,
    JSON_VALUE(payload, '$.particulars') AS particulars,
    JSON_VALUE(payload, '$.reference') AS reference,
    JSON_VALUE(payload, '$.status') AS status,
    SAFE_CAST(JSON_VALUE(payload, '$.total_amount') AS FLOAT64) AS total_amount,
    JSON_VALUE(payload, '$.type') AS type,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    JSON_VALUE(payload, '$.validation_errors') AS validation_errors,
    ingestion_time
FROM 
    {{ source('raw', 'xero_batch_payments') }}