{{ config(
    tags=['transformed', 'xero', 'purchase_orders']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.purchase_order_id') AS purchase_order_id,
    JSON_VALUE(payload, '$.attachments') AS attachments,
    JSON_VALUE(payload, '$.attention_to') AS attention_to,
    JSON_VALUE(payload, '$.branding_theme_id') AS branding_theme_id,
    JSON_VALUE(payload, '$.contact.contact_id') AS contact_id,
    JSON_VALUE(payload, '$.currency_code') AS currency_code,
    SAFE_CAST(JSON_VALUE(payload, '$.currency_rate') AS FLOAT64) AS currency_rate,
    SAFE_CAST(JSON_VALUE(payload, '$.date') AS DATE) AS date,
    JSON_VALUE(payload, '$.delivery_address') AS delivery_address,
    SAFE_CAST(JSON_VALUE(payload, '$.delivery_date') AS DATE) AS delivery_date,
    JSON_VALUE(payload, '$.delivery_instructions') AS delivery_instructions,
    SAFE_CAST(JSON_VALUE(payload, '$.expected_arrival_date') AS DATE) AS expected_arrival_date,
    SAFE_CAST(JSON_VALUE(payload, '$.has_attachments') AS BOOL) AS has_attachments,
    JSON_VALUE(payload, '$.line_amount_types') AS line_amount_types,
    JSON_VALUE(payload, '$.purchase_order_number') AS purchase_order_number,
    JSON_VALUE(payload, '$.reference') AS reference,
    SAFE_CAST(JSON_VALUE(payload, '$.sent_to_contact') AS BOOL) AS sent_to_contact,
    JSON_VALUE(payload, '$.status') AS status,
    JSON_VALUE(payload, '$.status_attribute_string') AS status_attribute_string,
    SAFE_CAST(JSON_VALUE(payload, '$.sub_total') AS FLOAT64) AS sub_total,
    JSON_VALUE(payload, '$.telephone') AS telephone,
    SAFE_CAST(JSON_VALUE(payload, '$.total') AS FLOAT64) AS total,
    SAFE_CAST(JSON_VALUE(payload, '$.total_discount') AS FLOAT64) AS total_discount,
    SAFE_CAST(JSON_VALUE(payload, '$.total_tax') AS FLOAT64) AS total_tax,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    JSON_VALUE(payload, '$.validation_errors') AS validation_errors,
    JSON_VALUE(payload, '$.warnings') AS warnings,
    ingestion_time
FROM 
    {{ source('raw', 'xero_purchase_orders') }}