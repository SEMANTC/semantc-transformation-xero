{{ config(
    tags=['transformed', 'xero', 'expense_claims', 'payments']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.expense_claim_id') AS expense_claim_id,
    JSON_VALUE(payment, '$.payment_id') AS payment_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_expense_claims') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.payments')) AS payment
WHERE 
    JSON_VALUE(payment, '$.payment_id') IS NOT NULL