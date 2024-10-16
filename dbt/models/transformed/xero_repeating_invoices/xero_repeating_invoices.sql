{{ config(
    tags=['transformed', 'xero', 'repeating_invoices']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.repeating_invoice_id') AS repeating_invoice_id,
    SAFE_CAST(JSON_VALUE(payload, '$.approved_for_sending') AS BOOL) AS approved_for_sending,
    JSON_VALUE(payload, '$.attachments') AS attachments,
    JSON_VALUE(payload, '$.branding_theme_id') AS branding_theme_id,
    JSON_VALUE(payload, '$.contact.contact_id') AS contact_id,
    JSON_VALUE(payload, '$.currency_code') AS currency_code,
    SAFE_CAST(JSON_VALUE(payload, '$.has_attachments') AS BOOL) AS has_attachments,
    JSON_VALUE(payload, '$.id') AS id,
    SAFE_CAST(JSON_VALUE(payload, '$.include_pdf') AS BOOL) AS include_pdf,
    JSON_VALUE(payload, '$.line_amount_types') AS line_amount_types,
    SAFE_CAST(JSON_VALUE(payload, '$.mark_as_sent') AS BOOL) AS mark_as_sent,
    JSON_VALUE(payload, '$.reference') AS reference,
    SAFE_CAST(JSON_VALUE(payload, '$.send_copy') AS BOOL) AS send_copy,
    JSON_VALUE(payload, '$.status') AS status,
    SAFE_CAST(JSON_VALUE(payload, '$.sub_total') AS FLOAT64) AS sub_total,
    SAFE_CAST(JSON_VALUE(payload, '$.total') AS FLOAT64) AS total,
    SAFE_CAST(JSON_VALUE(payload, '$.total_tax') AS FLOAT64) AS total_tax,
    JSON_VALUE(payload, '$.type') AS type,
    ingestion_time
FROM 
    {{ source('raw', 'xero_repeating_invoices') }}