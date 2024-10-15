{{ config(
    tags=['transformed', 'xero', 'batch_payments', 'payments']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.batch_payment_id') AS batch_payment_id,
    SAFE_CAST(JSON_VALUE(payment, '$.amount') AS FLOAT64) AS amount,
    JSON_VALUE(payment, '$.bank_account_number') AS bank_account_number,
    SAFE_CAST(JSON_VALUE(payment, '$.invoice.invoice_id') AS STRING) AS invoice_id,
    JSON_VALUE(payment, '$.payment_id') AS payment_id,
    JSON_VALUE(payment, '$.particulars') AS payment_particulars,
    JSON_VALUE(payment, '$.reference') AS payment_reference,
    ingestion_time
FROM 
    {{ source('raw', 'xero_batch_payments') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.payments')) AS payment
WHERE 
    JSON_VALUE(payment, '$.payment_id') IS NOT NULL