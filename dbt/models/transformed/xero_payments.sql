{{ config(
    tags=['transformed', 'xero', 'payments']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.payment_id') AS payment_id,
    JSON_VALUE(payload, '$.account.account_id') AS account_id,
    SAFE_CAST(JSON_VALUE(payload, '$.amount') AS FLOAT64) AS amount,
    SAFE_CAST(JSON_VALUE(payload, '$.bank_amount') AS FLOAT64) AS bank_amount,
    JSON_VALUE(payload, '$.bank_account_number') AS bank_account_number,
    JSON_VALUE(payload, '$.batch_payment_id') AS batch_payment_id,
    JSON_VALUE(payload, '$.code') AS code,
    JSON_VALUE(payload, '$.credit_note_number') AS credit_note_number,
    SAFE_CAST(JSON_VALUE(payload, '$.currency_rate') AS FLOAT64) AS currency_rate,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(payload, '$.date')) AS date,
    JSON_VALUE(payload, '$.details') AS details,
    SAFE_CAST(JSON_VALUE(payload, '$.has_account') AS BOOL) AS has_account,
    SAFE_CAST(JSON_VALUE(payload, '$.has_validation_errors') AS BOOL) AS has_validation_errors,
    JSON_VALUE(payload, '$.invoice.invoice_id') AS invoice_id,
    SAFE_CAST(JSON_VALUE(payload, '$.is_reconciled') AS BOOL) AS is_reconciled,
    JSON_VALUE(payload, '$.particulars') AS particulars,
    JSON_VALUE(payload, '$.payment_type') AS payment_type,
    JSON_VALUE(payload, '$.reference') AS reference,
    JSON_VALUE(payload, '$.status') AS status,
    JSON_VALUE(payload, '$.status_attribute_string') AS status_attribute_string,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S%Ez', JSON_VALUE(payload, '$.updated_date_utc')) AS updated_date_utc,
    ingestion_time
FROM 
    {{ source('raw', 'xero_payments') }}