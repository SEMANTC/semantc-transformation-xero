{{ config(
    tags=['transformed', 'xero', 'bank_transactions', 'line_items']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.bank_transaction_id') AS bank_transaction_id,
    JSON_VALUE(line_item, '$.line_item_id') AS line_item_id,
    JSON_VALUE(line_item, '$.account_code') AS account_code,
    JSON_VALUE(line_item, '$.description') AS description,
    JSON_VALUE(line_item, '$.item_code') AS item_code,
    SAFE_CAST(JSON_VALUE(line_item, '$.line_amount') AS FLOAT64) AS line_amount,
    SAFE_CAST(JSON_VALUE(line_item, '$.quantity') AS FLOAT64) AS quantity,
    SAFE_CAST(JSON_VALUE(line_item, '$.tax_amount') AS FLOAT64) AS tax_amount,
    JSON_VALUE(line_item, '$.tax_type') AS tax_type,
    SAFE_CAST(JSON_VALUE(line_item, '$.unit_amount') AS FLOAT64) AS unit_amount,
    ingestion_time
FROM 
    {{ source('raw', 'xero_bank_transactions') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.line_items')) AS line_item
WHERE 
    JSON_VALUE(line_item, '$.line_item_id') IS NOT NULL