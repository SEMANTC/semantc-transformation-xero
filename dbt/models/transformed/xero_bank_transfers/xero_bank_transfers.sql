{{ config(
    tags=['transformed', 'xero', 'bank_transfers']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.bank_transfer_id') AS bank_transfer_id,
    SAFE_CAST(JSON_VALUE(payload, '$.amount') AS FLOAT64) AS amount,
    SAFE_CAST(JSON_VALUE(payload, '$.currency_rate') AS FLOAT64) AS currency_rate,
    SAFE_CAST(JSON_VALUE(payload, '$.created_date_utc') AS TIMESTAMP) AS created_date_utc,
    JSON_VALUE(payload, '$.from_bank_account.account_id') AS from_bank_account_id,
    JSON_VALUE(payload, '$.from_bank_transaction_id') AS from_bank_transaction_id,
    SAFE_CAST(JSON_VALUE(payload, '$.from_is_reconciled') AS BOOL) AS from_is_reconciled,
    JSON_VALUE(payload, '$.reference') AS reference,
    SAFE_CAST(JSON_VALUE(payload, '$.has_attachments') AS BOOL) AS has_attachments,
    JSON_VALUE(payload, '$.to_bank_account.account_id') AS to_bank_account_id,
    JSON_VALUE(payload, '$.to_bank_transaction_id') AS to_bank_transaction_id,
    SAFE_CAST(JSON_VALUE(payload, '$.to_is_reconciled') AS BOOL) AS to_is_reconciled,
    SAFE_CAST(JSON_VALUE(payload, '$.date') AS DATE) AS date,
    ingestion_time
FROM 
    {{ source('raw', 'xero_bank_transfers') }}