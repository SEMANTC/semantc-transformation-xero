{{ config(
    tags=['transformed', 'xero', 'invoices']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.invoice_id') AS invoice_id,
    SAFE_CAST(JSON_VALUE(payload, '$.amount_credited') AS FLOAT64) AS amount_credited,
    SAFE_CAST(JSON_VALUE(payload, '$.amount_due') AS FLOAT64) AS amount_due,
    SAFE_CAST(JSON_VALUE(payload, '$.amount_paid') AS FLOAT64) AS amount_paid,
    JSON_VALUE(payload, '$.branding_theme_id') AS branding_theme_id,
    JSON_VALUE(payload, '$.cis_deduction') AS cis_deduction,
    JSON_VALUE(payload, '$.cis_rate') AS cis_rate,
    JSON_VALUE(payload, '$.contact.contact_id') AS contact_id,
    JSON_VALUE(payload, '$.currency_code') AS currency_code,
    SAFE_CAST(JSON_VALUE(payload, '$.currency_rate') AS FLOAT64) AS currency_rate,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(payload, '$.date')) AS date,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(payload, '$.due_date')) AS due_date,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(payload, '$.expected_payment_date')) AS expected_payment_date,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(payload, '$.fully_paid_on_date')) AS fully_paid_on_date,
    SAFE_CAST(JSON_VALUE(payload, '$.has_attachments') AS BOOL) AS has_attachments,
    SAFE_CAST(JSON_VALUE(payload, '$.has_errors') AS BOOL) AS has_errors,
    JSON_VALUE(payload, '$.invoice_number') AS invoice_number,
    SAFE_CAST(JSON_VALUE(payload, '$.is_discounted') AS BOOL) AS is_discounted,
    JSON_VALUE(payload, '$.line_amount_types') AS line_amount_types,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(payload, '$.planned_payment_date')) AS planned_payment_date,
    JSON_VALUE(payload, '$.reference') AS reference,
    JSON_VALUE(payload, '$.repeating_invoice_id') AS repeating_invoice_id,
    JSON_VALUE(payload, '$.sent_to_contact') AS sent_to_contact,
    JSON_VALUE(payload, '$.status') AS status,
    JSON_VALUE(payload, '$.status_attribute_string') AS status_attribute_string,
    SAFE_CAST(JSON_VALUE(payload, '$.sub_total') AS FLOAT64) AS sub_total,
    SAFE_CAST(JSON_VALUE(payload, '$.total') AS FLOAT64) AS total,
    SAFE_CAST(JSON_VALUE(payload, '$.total_discount') AS FLOAT64) AS total_discount,
    SAFE_CAST(JSON_VALUE(payload, '$.total_tax') AS FLOAT64) AS total_tax,
    JSON_VALUE(payload, '$.type') AS type,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S%Ez', JSON_VALUE(payload, '$.updated_date_utc')) AS updated_date_utc,
    JSON_VALUE(payload, '$.url') AS url,
    ingestion_time
FROM 
    {{ source('raw', 'xero_invoices') }}