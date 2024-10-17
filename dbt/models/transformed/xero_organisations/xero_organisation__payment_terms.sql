{{ config(
    tags=['transformed', 'xero', 'organisations', 'payment_terms']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.organisation_id') AS organisation_id,
    JSON_VALUE(payment_term, '$.bills.day') AS bills_day,
    JSON_VALUE(payment_term, '$.bills.type') AS bills_type,
    JSON_VALUE(payment_term, '$.sales.day') AS sales_day,
    JSON_VALUE(payment_term, '$.sales.type') AS sales_type,
    ingestion_time
FROM 
    {{ source('raw', 'xero_organisations') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.payment_terms')) AS payment_term
WHERE 
    JSON_VALUE(payment_term, '$.bills.type') IS NOT NULL OR
    JSON_VALUE(payment_term, '$.sales.type') IS NOT NULL