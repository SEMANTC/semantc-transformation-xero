{{ config(
    tags=['transformed', 'xero', 'accounts']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.account_id') AS account_id,
    SAFE_CAST(JSON_VALUE(payload, '$.add_to_watchlist') AS BOOL) AS add_to_watchlist,
    JSON_VALUE(payload, '$.bank_account_number') AS bank_account_number,
    JSON_VALUE(payload, '$.bank_account_type') AS bank_account_type,
    JSON_VALUE(payload, '$._class') AS class,
    JSON_VALUE(payload, '$.code') AS code,
    JSON_VALUE(payload, '$.currency_code') AS currency_code,
    JSON_VALUE(payload, '$.description') AS description,
    SAFE_CAST(JSON_VALUE(payload, '$.enable_payments_to_account') AS BOOL) AS enable_payments_to_account,
    SAFE_CAST(JSON_VALUE(payload, '$.has_attachments') AS BOOL) AS has_attachments,
    JSON_VALUE(payload, '$.name') AS name,
    JSON_VALUE(payload, '$.reporting_code') AS reporting_code,
    JSON_VALUE(payload, '$.reporting_code_name') AS reporting_code_name,
    SAFE_CAST(JSON_VALUE(payload, '$.show_in_expense_claims') AS BOOL) AS show_in_expense_claims,
    JSON_VALUE(payload, '$.status') AS status,
    JSON_VALUE(payload, '$.system_account') AS system_account,
    JSON_VALUE(payload, '$.tax_type') AS tax_type,
    JSON_VALUE(payload, '$.type') AS type,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    JSON_VALUE(payload, '$.validation_errors') AS validation_errors,
    ingestion_time
FROM 
    {{ source('raw', 'xero_accounts') }}
