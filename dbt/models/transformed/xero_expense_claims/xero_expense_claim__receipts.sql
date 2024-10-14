{{ config(
    tags=['transformed', 'xero', 'expense_claims', 'receipts']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.expense_claim_id') AS expense_claim_id,
    JSON_VALUE(receipt, '$.receipt_id') AS receipt_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_expense_claims') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.receipts')) AS receipt
WHERE 
    JSON_VALUE(receipt, '$.receipt_id') IS NOT NULL