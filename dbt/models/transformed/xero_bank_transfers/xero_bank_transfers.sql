{{ config(
    tags=['transformed', 'xero', 'bank_transfers'],
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.bank_transfer_id') AS bank_transfer_id,
    JSON_VALUE(payload, '$.amount') AS amount,
    JSON_VALUE(payload, '$.currency_rate') AS currency_rate,
    JSON_VALUE(payload, '$.created_date_utc') AS created_date_utc,
    JSON_VALUE(payload, '$.from_bank_account.account_id') AS from_bank_account_id,
    JSON_VALUE(payload, '$.from_bank_transaction_id') AS from_bank_transaction_id,
    JSON_VALUE(payload, '$.from_is_reconciled') AS from_is_reconciled,
    JSON_VALUE(payload, '$.reference') AS reference,
    JSON_VALUE(payload, '$.has_attachments') AS has_attachments,
    JSON_VALUE(payload, '$.to_bank_account.account_id') AS to_bank_account_id,
    JSON_VALUE(payload, '$.to_bank_transaction_id') AS to_bank_transaction_id,
    JSON_VALUE(payload, '$.to_is_reconciled') AS to_is_reconciled,
    JSON_VALUE(payload, '$.date') AS date,
    ingestion_time
FROM 
    {{ source('raw', 'xero_bank_transfers') }}