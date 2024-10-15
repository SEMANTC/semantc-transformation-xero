{{ config(
    tags=['transformed', 'xero', 'expense_claims']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.expense_claim_id') AS expense_claim_id,
    SAFE_CAST(JSON_VALUE(payload, '$.amount_due') AS FLOAT64) AS amount_due,
    SAFE_CAST(JSON_VALUE(payload, '$.amount_paid') AS FLOAT64) AS amount_paid,
    JSON_VALUE(payload, '$.currency_code') AS currency_code,
    SAFE_CAST(JSON_VALUE(payload, '$.currency_rate') AS FLOAT64) AS currency_rate,
    SAFE_CAST(JSON_VALUE(payload, '$.fully_paid_on_date') AS DATE) AS fully_paid_on_date,
    SAFE_CAST(JSON_VALUE(payload, '$.payment_due_date') AS DATE) AS payment_due_date,
    JSON_VALUE(payload, '$.receipt_id') AS receipt_id,
    SAFE_CAST(JSON_VALUE(payload, '$.reporting_date') AS DATE) AS reporting_date,
    JSON_VALUE(payload, '$.status') AS status,
    SAFE_CAST(JSON_VALUE(payload, '$.total') AS FLOAT64) AS total,
    JSON_VALUE(payload, '$.type') AS type,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    ingestion_time
FROM 
    {{ source('raw', 'xero_expense_claims') }}
WHERE
    JSON_VALUE(payload, '$.expense_claim_id') IS NOT NULL