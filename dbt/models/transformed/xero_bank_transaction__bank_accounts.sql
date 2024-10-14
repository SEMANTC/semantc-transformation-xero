{{ config(
    tags=['transformed', 'xero', 'bank_transactions', 'bank_accounts']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.bank_transaction_id') AS bank_transaction_id,
    JSON_VALUE(bank_account, '$.account_id') AS account_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_bank_transactions') }},
    UNNEST([JSON_QUERY(payload, '$.bank_account')]) AS bank_account
WHERE 
    JSON_VALUE(bank_account, '$.account_id') IS NOT NULL