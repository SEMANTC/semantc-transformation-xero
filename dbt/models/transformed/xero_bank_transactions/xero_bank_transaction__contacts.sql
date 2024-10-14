{{ config(
    tags=['transformed', 'xero', 'bank_transactions', 'contacts']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.bank_transaction_id') AS bank_transaction_id,
    JSON_VALUE(contact, '$.contact_id') AS contact_id,
    ingestion_time
FROM 
    {{ source('raw', 'xero_bank_transactions') }},
    UNNEST([JSON_QUERY(payload, '$.contact')]) AS contact
WHERE 
    JSON_VALUE(contact, '$.contact_id') IS NOT NULL