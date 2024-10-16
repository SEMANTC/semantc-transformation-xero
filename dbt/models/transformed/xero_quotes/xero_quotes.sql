{{ config(
    tags=['transformed', 'xero', 'quotes']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.quote_id') AS quote_id,
    JSON_VALUE(payload, '$.branding_theme_id') AS branding_theme_id,
    JSON_VALUE(payload, '$.contact.contact_id') AS contact_id,
    JSON_VALUE(payload, '$.currency_code') AS currency_code,
    SAFE_CAST(JSON_VALUE(payload, '$.currency_rate') AS FLOAT64) AS currency_rate,
    SAFE_CAST(JSON_VALUE(payload, '$.date') AS DATE) AS date,
    JSON_VALUE(payload, '$.date_string') AS date_string,
    SAFE_CAST(JSON_VALUE(payload, '$.expiry_date') AS DATE) AS expiry_date,
    JSON_VALUE(payload, '$.expiry_date_string') AS expiry_date_string,
    JSON_VALUE(payload, '$.line_amount_types') AS line_amount_types,
    JSON_VALUE(payload, '$.quote_number') AS quote_number,
    JSON_VALUE(payload, '$.reference') AS reference,
    JSON_VALUE(payload, '$.status') AS status,
    JSON_VALUE(payload, '$.status_attribute_string') AS status_attribute_string,
    SAFE_CAST(JSON_VALUE(payload, '$.sub_total') AS FLOAT64) AS sub_total,
    JSON_VALUE(payload, '$.summary') AS summary,
    JSON_VALUE(payload, '$.terms') AS terms,
    JSON_VALUE(payload, '$.title') AS title,
    SAFE_CAST(JSON_VALUE(payload, '$.total') AS FLOAT64) AS total,
    SAFE_CAST(JSON_VALUE(payload, '$.total_discount') AS FLOAT64) AS total_discount,
    SAFE_CAST(JSON_VALUE(payload, '$.total_tax') AS FLOAT64) AS total_tax,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    JSON_VALUE(payload, '$.validation_errors') AS validation_errors,
    ingestion_time
FROM 
    {{ source('raw', 'xero_quotes') }}