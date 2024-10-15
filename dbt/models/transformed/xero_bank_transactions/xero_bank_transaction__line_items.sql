{{ config(
    tags=['transformed', 'xero', 'bank_transactions', 'line_items']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.bank_transaction_id') AS bank_transaction_id,
    JSON_VALUE(line_item, '$.line_item_id') AS line_item_id,
    JSON_VALUE(line_item, '$.account_code') AS line_item_account_code,
    SAFE_CAST(JSON_VALUE(line_item, '$.created_date_utc') AS TIMESTAMP) AS line_item_created_date_utc,
    JSON_VALUE(line_item, '$.description') AS line_item_description,
    JSON_VALUE(line_item, '$.item_code') AS line_item_item_code,
    SAFE_CAST(JSON_VALUE(line_item, '$.line_amount') AS FLOAT64) AS line_item_line_amount,
    SAFE_CAST(JSON_VALUE(line_item, '$.quantity') AS FLOAT64) AS line_item_quantity,
    SAFE_CAST(JSON_VALUE(line_item, '$.tax_amount') AS FLOAT64) AS line_item_tax_amount,
    JSON_VALUE(line_item, '$.tax_type') AS line_item_tax_type,
    JSON_VALUE(line_item, '$.tracking_category') AS line_item_tracking_category,
    JSON_VALUE(line_item, '$.tracking_category_option') AS line_item_tracking_category_option,
    SAFE_CAST(JSON_VALUE(line_item, '$.unit_amount') AS FLOAT64) AS line_item_unit_amount,
    SAFE_CAST(JSON_VALUE(line_item, '$.updated_date_utc') AS TIMESTAMP) AS line_item_updated_date_utc,
    ingestion_time
FROM 
    {{ source('raw', 'xero_bank_transactions') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.line_items')) AS line_item
WHERE 
    JSON_VALUE(line_item, '$.description') IS NOT NULL