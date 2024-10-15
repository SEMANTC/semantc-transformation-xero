{{ config(
    tags=['transformed', 'xero', 'credit_notes']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.credit_note_id') AS credit_note_id,
    JSON_VALUE(payload, '$.credit_note_number') AS credit_note_number,
    JSON_VALUE(payload, '$.currency_code') AS currency_code,
    SAFE_CAST(JSON_VALUE(payload, '$.currency_rate') AS FLOAT64) AS currency_rate,
    SAFE_CAST(JSON_VALUE(payload, '$.date') AS DATE) AS date,
    SAFE_CAST(JSON_VALUE(payload, '$.due_date') AS DATE) AS due_date,
    SAFE_CAST(JSON_VALUE(payload, '$.applied_amount') AS FLOAT64) AS applied_amount,
    SAFE_CAST(JSON_VALUE(payload, '$.cis_deduction') AS FLOAT64) AS cis_deduction,
    SAFE_CAST(JSON_VALUE(payload, '$.cis_rate') AS FLOAT64) AS cis_rate,
    SAFE_CAST(JSON_VALUE(payload, '$.fully_paid_on_date') AS DATE) AS fully_paid_on_date,
    JSON_VALUE(payload, '$.has_attachments') AS has_attachments,
    JSON_VALUE(payload, '$.has_errors') AS has_errors,
    SAFE_CAST(JSON_VALUE(payload, '$.remaining_credit') AS FLOAT64) AS remaining_credit,
    SAFE_CAST(JSON_VALUE(payload, '$.sent_to_contact') AS BOOL) AS sent_to_contact,
    SAFE_CAST(JSON_VALUE(payload, '$.sub_total') AS FLOAT64) AS sub_total,
    SAFE_CAST(JSON_VALUE(payload, '$.total') AS FLOAT64) AS total,
    SAFE_CAST(JSON_VALUE(payload, '$.total_tax') AS FLOAT64) AS total_tax,
    JSON_VALUE(payload, '$.type') AS type,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    JSON_VALUE(payload, '$.reference') AS reference,
    ingestion_time
FROM 
    {{ source('raw', 'xero_credit_notes') }}