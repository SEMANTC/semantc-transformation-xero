{{ config(
    tags=['transformed', 'xero', 'organisations']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.organisation_id') AS organisation_id,
    JSON_VALUE(payload, '$.api_key') AS api_key,
    JSON_VALUE(payload, '$.name') AS name,
    JSON_VALUE(payload, '$.legal_name') AS legal_name,
    SAFE_CAST(JSON_VALUE(payload, '$.pays_tax') AS BOOL) AS pays_tax,
    JSON_VALUE(payload, '$.version') AS version,
    JSON_VALUE(payload, '$.organisation_type') AS organisation_type,
    JSON_VALUE(payload, '$.base_currency') AS base_currency,
    JSON_VALUE(payload, '$.country_code') AS country_code,
    SAFE_CAST(JSON_VALUE(payload, '$.is_demo_company') AS BOOL) AS is_demo_company,
    JSON_VALUE(payload, '$.organisation_status') AS organisation_status,
    JSON_VALUE(payload, '$.registration_number') AS registration_number,
    JSON_VALUE(payload, '$.employer_identification_number') AS employer_identification_number,
    JSON_VALUE(payload, '$.tax_number') AS tax_number,
    SAFE_CAST(JSON_VALUE(payload, '$.financial_year_end_day') AS INT64) AS financial_year_end_day,
    SAFE_CAST(JSON_VALUE(payload, '$.financial_year_end_month') AS INT64) AS financial_year_end_month,
    JSON_VALUE(payload, '$.sales_tax_basis') AS sales_tax_basis,
    JSON_VALUE(payload, '$.sales_tax_period') AS sales_tax_period,
    JSON_VALUE(payload, '$.default_sales_tax') AS default_sales_tax,
    JSON_VALUE(payload, '$.default_purchases_tax') AS default_purchases_tax,
    SAFE_CAST(JSON_VALUE(payload, '$.period_lock_date') AS DATE) AS period_lock_date,
    SAFE_CAST(JSON_VALUE(payload, '$.end_of_year_lock_date') AS DATE) AS end_of_year_lock_date,
    SAFE_CAST(JSON_VALUE(payload, '$.created_date_utc') AS TIMESTAMP) AS created_date_utc,
    JSON_VALUE(payload, '$.timezone') AS timezone,
    JSON_VALUE(payload, '$.organisation_entity_type') AS organisation_entity_type,
    JSON_VALUE(payload, '$.short_code') AS short_code,
    JSON_VALUE(payload, '$._class') AS class,
    JSON_VALUE(payload, '$.edition') AS edition,
    JSON_VALUE(payload, '$.line_of_business') AS line_of_business,
    SAFE_CAST(JSON_VALUE(payload, '$.show_on_cash_basis_reports') AS BOOL) AS show_on_cash_basis_reports,
    SAFE_CAST(JSON_VALUE(payload, '$.has_attachments') AS BOOL) AS has_attachments,
    JSON_VALUE(payload, '$.url') AS url,
    JSON_VALUE(payload, '$.validation_errors') AS validation_errors,
    JSON_VALUE(payload, '$.warnings') AS warnings,
    ingestion_time
FROM 
    {{ source('raw', 'xero_organisations') }}
WHERE
    JSON_VALUE(payload, '$.organisation_id') IS NOT NULL