{{ config(
    tags=['transformed', 'xero', 'credit_notes', 'line_items']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.credit_note_id') AS credit_note_id,
    JSON_VALUE(line_item, '$.line_item_id') AS line_item_id,
    JSON_VALUE(line_item, '$.description') AS description,
    SAFE_CAST(JSON_VALUE(line_item, '$.quantity') AS FLOAT64) AS quantity,
    SAFE_CAST(JSON_VALUE(line_item, '$.unit_amount') AS FLOAT64) AS unit_amount,
    JSON_VALUE(line_item, '$.account_code') AS account_code,
    JSON_VALUE(line_item, '$.tax_type') AS tax_type,
    SAFE_CAST(JSON_VALUE(line_item, '$.tax_amount') AS FLOAT64) AS tax_amount,
    SAFE_CAST(JSON_VALUE(line_item, '$.line_amount') AS FLOAT64) AS line_amount,
    ingestion_time
FROM 
    {{ source('raw', 'xero_credit_notes') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.line_items')) AS line_item
WHERE 
    JSON_VALUE(line_item, '$.line_item_id') IS NOT NULL