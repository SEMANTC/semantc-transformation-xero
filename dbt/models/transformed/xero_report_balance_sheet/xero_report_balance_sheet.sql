{{ config(
    tags=['transformed', 'xero', 'reports', 'balance_sheet', 'flattened']
) }}

WITH extracted_rows AS (
    SELECT
        JSON_VALUE(payload, '$.report_id') AS report_id,
        JSON_VALUE(payload, '$.report_name') AS report_name,
        JSON_VALUE(payload, '$.report_type') AS report_type,
        JSON_VALUE(payload, '$.report_date') AS report_date,
        SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
        row,
        CAST(row_index AS INT64) AS row_index
    FROM 
        {{ source('raw', 'xero_report_balance_sheet') }},
        UNNEST(JSON_QUERY_ARRAY(payload, '$.rows')) AS row WITH OFFSET AS row_index
),

flattened_rows AS (
    SELECT
        report_id,
        report_name,
        report_type,
        report_date,
        updated_date_utc,
        row_index,
        JSON_VALUE(row, '$.row_type') AS row_type,
        JSON_VALUE(row, '$.title') AS title,
        cell,
        CAST(cell_index AS INT64) AS cell_index
    FROM 
        extracted_rows,
        UNNEST(JSON_QUERY_ARRAY(row, '$.cells')) AS cell WITH OFFSET AS cell_index
)

SELECT
    report_id,
    report_name,
    report_type,
    report_date,
    updated_date_utc,
    row_index,
    row_type,
    title,
    MAX(CASE WHEN cell_index = 0 THEN JSON_VALUE(cell, '$.value') END) AS description,
    MAX(CASE WHEN cell_index = 1 THEN SAFE_CAST(JSON_VALUE(cell, '$.value') AS FLOAT64) END) AS current_year_value,
    MAX(CASE WHEN cell_index = 2 THEN SAFE_CAST(JSON_VALUE(cell, '$.value') AS FLOAT64) END) AS previous_year_value,
    MAX(JSON_VALUE(cell, '$.attributes[0].value')) AS account_id,
    MAX(JSON_VALUE(cell, '$.attributes[1].value')) AS from_date,
    MAX(JSON_VALUE(cell, '$.attributes[2].value')) AS to_date
FROM 
    flattened_rows
GROUP BY
    report_id, report_name, report_type, report_date, updated_date_utc,
    row_index, row_type, title
ORDER BY
    row_index