{{ config(
    tags=['transformed', 'xero', 'bank_transactions']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.bank_transaction_id') AS bank_transaction_id,
    JSON_VALUE(payload, '$.currency_code') AS currency_code,
    SAFE_CAST(JSON_VALUE(payload, '$.currency_rate') AS FLOAT64) AS currency_rate,
    SAFE_CAST(JSON_VALUE(payload, '$.date') AS DATE) AS date,
    SAFE_CAST(JSON_VALUE(payload, '$.has_attachments') AS BOOL) AS has_attachments,
    SAFE_CAST(JSON_VALUE(payload, '$.is_reconciled') AS BOOL) AS is_reconciled,
    JSON_VALUE(payload, '$.line_amount_types') AS line_amount_types,
    JSON_VALUE(payload, '$.overpayment_id') AS overpayment_id,
    JSON_VALUE(payload, '$.prepayment_id') AS prepayment_id,
    JSON_VALUE(payload, '$.reference') AS reference,
    JSON_VALUE(payload, '$.status') AS status,
    JSON_VALUE(payload, '$.status_attribute_string') AS status_attribute_string,
    SAFE_CAST(JSON_VALUE(payload, '$.sub_total') AS FLOAT64) AS sub_total,
    SAFE_CAST(JSON_VALUE(payload, '$.total') AS FLOAT64) AS total,
    SAFE_CAST(JSON_VALUE(payload, '$.total_tax') AS FLOAT64) AS total_tax,
    JSON_VALUE(payload, '$.type') AS type,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    JSON_VALUE(payload, '$.url') AS url,
    JSON_VALUE(payload, '$.validation_errors') AS validation_errors,
    ingestion_time
FROM 
    {{ source('raw', 'xero_bank_transactions') }}