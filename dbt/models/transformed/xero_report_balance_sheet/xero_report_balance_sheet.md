{{ config(
    tags=['transformed', 'xero', 'reports', 'balance_sheet', 'flattened'],
) }}

WITH base AS (
  SELECT
    JSON_VALUE(payload, '$.report_id') AS report_id,
    JSON_VALUE(payload, '$.report_name') AS report_name,
    JSON_VALUE(payload, '$.report_type') AS report_type,
    JSON_VALUE(payload, '$.report_date') AS report_date,
    SAFE_CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
    JSON_QUERY_ARRAY(payload, '$.rows') AS rows_array
  FROM 
    {{ source('raw', 'xero_report_balance_sheet') }}
),

level1_rows AS (
  SELECT
    report_id,
    report_name,
    report_type,
    report_date,
    updated_date_utc,
    CAST(1 AS INT64) AS level,
    JSON_VALUE(row_json, '$.row_type') AS row_type,
    JSON_VALUE(row_json, '$.title') AS title,
    row_json,
    JSON_QUERY_ARRAY(row_json, '$.rows') AS nested_rows,
    JSON_QUERY_ARRAY(row_json, '$.cells') AS cells_array,
    CAST(NULL AS STRING) AS parent_title  -- Explicitly cast NULL to STRING
  FROM base, UNNEST(rows_array) AS row_json
),

level2_rows AS (
  SELECT
    report_id,
    report_name,
    report_type,
    report_date,
    updated_date_utc,
    2 AS level,
    JSON_VALUE(nested_row_json, '$.row_type') AS row_type,
    JSON_VALUE(nested_row_json, '$.title') AS title,
    nested_row_json AS row_json,
    JSON_QUERY_ARRAY(nested_row_json, '$.rows') AS nested_rows,
    JSON_QUERY_ARRAY(nested_row_json, '$.cells') AS cells_array,
    level1_rows.title AS parent_title
  FROM level1_rows, UNNEST(nested_rows) AS nested_row_json
  WHERE nested_rows IS NOT NULL
),

level3_rows AS (
  SELECT
    report_id,
    report_name,
    report_type,
    report_date,
    updated_date_utc,
    3 AS level,
    JSON_VALUE(nested_row_json, '$.row_type') AS row_type,
    JSON_VALUE(nested_row_json, '$.title') AS title,
    nested_row_json AS row_json,
    JSON_QUERY_ARRAY(nested_row_json, '$.rows') AS nested_rows,
    JSON_QUERY_ARRAY(nested_row_json, '$.cells') AS cells_array,
    level2_rows.title AS parent_title
  FROM level2_rows, UNNEST(nested_rows) AS nested_row_json
  WHERE nested_rows IS NOT NULL
),

all_rows AS (
  SELECT * FROM level1_rows
  UNION ALL
  SELECT * FROM level2_rows
  UNION ALL
  SELECT * FROM level3_rows
),

flattened_cells AS (
  SELECT
    report_id,
    report_name,
    report_type,
    report_date,
    updated_date_utc,
    level,
    row_type,
    COALESCE(title, parent_title) AS title,
    cell,
    CAST(cell_index AS INT64) AS cell_index,
    JSON_QUERY_ARRAY(cell, '$.attributes') AS attributes
  FROM 
    all_rows,
    UNNEST(cells_array) AS cell WITH OFFSET AS cell_index
  WHERE cells_array IS NOT NULL
),

attributes_extracted AS (
  SELECT
    *,
    (SELECT JSON_VALUE(attr, '$.value') FROM UNNEST(attributes) AS attr WHERE JSON_VALUE(attr, '$.id') = 'account') AS account_id,
    (SELECT JSON_VALUE(attr, '$.value') FROM UNNEST(attributes) AS attr WHERE JSON_VALUE(attr, '$.id') = 'fromDate') AS from_date,
    (SELECT JSON_VALUE(attr, '$.value') FROM UNNEST(attributes) AS attr WHERE JSON_VALUE(attr, '$.id') = 'toDate') AS to_date
  FROM flattened_cells
)

SELECT
  report_id,
  report_name,
  report_type,
  report_date,
  updated_date_utc,
  level,
  row_type,
  title,
  MAX(CASE WHEN cell_index = 0 THEN JSON_VALUE(cell, '$.value') END) AS description,
  MAX(CASE WHEN cell_index = 1 THEN SAFE_CAST(REPLACE(JSON_VALUE(cell, '$.value'), ',', '') AS FLOAT64) END) AS current_year_value,
  MAX(CASE WHEN cell_index = 2 THEN SAFE_CAST(REPLACE(JSON_VALUE(cell, '$.value'), ',', '') AS FLOAT64) END) AS previous_year_value,
  MAX(account_id) AS account_id,
  MAX(from_date) AS from_date,
  MAX(to_date) AS to_date
FROM 
  attributes_extracted
GROUP BY
  report_id, report_name, report_type, report_date, updated_date_utc,
  level, row_type, title
ORDER BY
  level, title