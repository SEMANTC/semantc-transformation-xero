{{ config(
    tags=['transformed', 'xero', 'overpayments']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.overpayment_id') AS overpayment_id,
    SAFE_CAST(JSON_VALUE(payload, '$.applied_amount') AS FLOAT64) AS applied_amount,
    JSON_VALUE(payload, '$.contact.contact_id') AS contact_id,
    JSON_VALUE(payload, '$.currency_code') AS currency_code,
    SAFE_CAST(JSON_VALUE(payload, '$.currency_rate') AS FLOAT64) AS currency_rate,
    SAFE_CAST(JSON_VALUE(payload, '$.date') AS DATE) AS date,
    SAFE_CAST(JSON_VALUE(payload, '$.has_attachments') AS BOOL) AS has_attachments,
    JSON_VALUE(payload, '$.line_amount_types') AS line_amount_types,
    SAFE_CAST(JSON_VALUE(payload, '$.remaining_credit') AS FLOAT64) AS remaining_credit,
    JSON_VALUE(payload, '$.status') AS status,
    SAFE_CAST(JSON_VALUE(payload, '$.sub_total') AS FLOAT64) AS sub_total,
    SAFE_CAST(JSON_VALUE(payload, '$.total') AS FLOAT64) AS total,
    SAFE_CAST(JSON_VALUE(payload, '$.total_tax') AS FLOAT64) AS total_tax,
    JSON_VALUE(payload, '$.type') AS type,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    ingestion_time
FROM 
    {{ source('raw', 'xero_overpayments') }}