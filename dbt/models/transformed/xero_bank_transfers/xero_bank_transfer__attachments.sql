{{ config(
    tags=['transformed', 'xero', 'bank_transfers', 'attachments']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.bank_transfer_id') AS bank_transfer_id,
    JSON_VALUE(attachment, '$.attachment_id') AS attachment_id,
    JSON_VALUE(attachment, '$.file_name') AS file_name,
    JSON_VALUE(attachment, '$.file_url') AS file_url,
    SAFE_CAST(JSON_VALUE(attachment, '$.created_date_utc') AS TIMESTAMP) AS attachment_created_date_utc,
    SAFE_CAST(JSON_VALUE(attachment, '$.updated_date_utc') AS TIMESTAMP) AS attachment_updated_date_utc,
    ingestion_time
FROM 
    {{ source('raw', 'xero_bank_transfers') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.attachments')) AS attachment
WHERE 
    JSON_VALUE(attachment, '$.attachment_id') IS NOT NULL