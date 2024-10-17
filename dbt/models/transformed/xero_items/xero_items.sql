{{ config(
    tags=['transformed', 'xero', 'items']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.item_id') AS item_id,
    JSON_VALUE(payload, '$.code') AS code,
    JSON_VALUE(payload, '$.description') AS description,
    SAFE_CAST(JSON_VALUE(payload, '$.inventory_asset_account_code') AS STRING) AS inventory_asset_account_code,
    SAFE_CAST(JSON_VALUE(payload, '$.is_purchased') AS BOOL) AS is_purchased,
    SAFE_CAST(JSON_VALUE(payload, '$.is_sold') AS BOOL) AS is_sold,
    SAFE_CAST(JSON_VALUE(payload, '$.is_tracked_as_inventory') AS BOOL) AS is_tracked_as_inventory,
    JSON_VALUE(payload, '$.name') AS name,
    JSON_VALUE(payload, '$.purchase_description') AS purchase_description,
    JSON_VALUE(payload, '$.purchase_details.account_code') AS purchase_account_code,
    JSON_VALUE(payload, '$.purchase_details.cogs_account_code') AS purchase_cogs_account_code,
    JSON_VALUE(payload, '$.purchase_details.tax_type') AS purchase_tax_type,
    SAFE_CAST(JSON_VALUE(payload, '$.purchase_details.unit_price') AS FLOAT64) AS purchase_unit_price,
    SAFE_CAST(JSON_VALUE(payload, '$.quantity_on_hand') AS FLOAT64) AS quantity_on_hand,
    JSON_VALUE(payload, '$.sales_details.account_code') AS sales_account_code,
    JSON_VALUE(payload, '$.sales_details.tax_type') AS sales_tax_type,
    SAFE_CAST(JSON_VALUE(payload, '$.sales_details.unit_price') AS FLOAT64) AS sales_unit_price,
    JSON_VALUE(payload, '$.status_attribute_string') AS status_attribute_string,
    SAFE_CAST(JSON_VALUE(payload, '$.total_cost_pool') AS FLOAT64) AS total_cost_pool,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    JSON_VALUE(payload, '$.validation_errors') AS validation_errors,
    ingestion_time
FROM 
    {{ source('raw', 'xero_items') }}