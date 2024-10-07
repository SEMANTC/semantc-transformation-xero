{{ config(
    tags=['transformed', 'xero', 'contacts']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.contact_id') AS contact_id,
    JSON_VALUE(payload, '$.account_number') AS account_number,
    JSON_VALUE(payload, '$.accounts_payable_tax_type') AS accounts_payable_tax_type,
    JSON_VALUE(payload, '$.accounts_receivable_tax_type') AS accounts_receivable_tax_type,
    JSON_VALUE(payload, '$.bank_account_details') AS bank_account_details,
    JSON_VALUE(payload, '$.contact_number') AS contact_number,
    JSON_VALUE(payload, '$.contact_status') AS contact_status,
    JSON_VALUE(payload, '$.default_currency') AS default_currency,
    SAFE_CAST(JSON_VALUE(payload, '$.discount') AS FLOAT64) AS discount,
    JSON_VALUE(payload, '$.email_address') AS email_address,
    JSON_VALUE(payload, '$.first_name') AS first_name,
    SAFE_CAST(JSON_VALUE(payload, '$.has_attachments') AS BOOL) AS has_attachments,
    SAFE_CAST(JSON_VALUE(payload, '$.has_validation_errors') AS BOOL) AS has_validation_errors,
    SAFE_CAST(JSON_VALUE(payload, '$.is_customer') AS BOOL) AS is_customer,
    SAFE_CAST(JSON_VALUE(payload, '$.is_supplier') AS BOOL) AS is_supplier,
    JSON_VALUE(payload, '$.last_name') AS last_name,
    JSON_VALUE(payload, '$.name') AS name,
    JSON_VALUE(payload, '$.skype_user_name') AS skype_user_name,
    JSON_VALUE(payload, '$.tax_number') AS tax_number,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    JSON_VALUE(payload, '$.website') AS website,
    ingestion_time
FROM 
    {{ source('raw', 'xero_contacts') }}