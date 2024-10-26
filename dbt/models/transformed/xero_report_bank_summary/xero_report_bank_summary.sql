{{ config(
    tags=['transformed', 'xero', 'reports', 'bank_summary']
) }}

WITH report_base AS (
    SELECT
        JSON_VALUE(payload, '$.report_id') AS report_id,
        JSON_VALUE(payload, '$.report_name') AS report_name,
        JSON_VALUE(payload, '$.report_type') AS report_type,
        PARSE_DATE('%d %B %Y', JSON_VALUE(payload, '$.report_date')) AS report_date,
        TIMESTAMP(JSON_VALUE(payload, '$.updated_date_utc')) AS updated_date_utc,
        ARRAY(
            SELECT value 
            FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.report_titles')) AS value
        ) AS report_titles,
        payload,
        ingestion_time
    FROM 
        {{ source('raw', 'xero_report_bank_summary') }}
),

rows_parsed AS (
    -- section headers (if any)
    SELECT
        base.report_id,
        base.report_date,
        base.report_name,
        base.report_type,
        base.updated_date_utc,
        base.report_titles,
        JSON_VALUE(r, '$.row_type') AS row_type,
        JSON_VALUE(r, '$.title') AS section_title,
        CAST(NULL AS STRING) AS parent_section,
        0 AS level,
        CAST(NULL AS STRING) AS row_label,
        CAST(NULL AS FLOAT64) AS opening_balance,
        CAST(NULL AS FLOAT64) AS cash_received,
        CAST(NULL AS FLOAT64) AS cash_spent,
        CAST(NULL AS FLOAT64) AS closing_balance,
        CAST(NULL AS STRING) AS account_id,
        base.ingestion_time
    FROM 
        report_base base,
        UNNEST(JSON_QUERY_ARRAY(payload, '$.rows')) AS r
    WHERE 
        JSON_VALUE(r, '$.row_type') = 'Section'

    UNION ALL

    -- line items within sections
    SELECT
        base.report_id,
        base.report_date,
        base.report_name,
        base.report_type,
        base.updated_date_utc,
        base.report_titles,
        JSON_VALUE(row_item, '$.row_type') AS row_type,
        JSON_VALUE(r, '$.title') AS section_title,
        JSON_VALUE(r, '$.title') AS parent_section,
        1 AS level,
        JSON_VALUE(cells, '$.value') AS row_label,
        SAFE_CAST(JSON_VALUE(cells_open, '$.value') AS FLOAT64) AS opening_balance,
        SAFE_CAST(JSON_VALUE(cells_received, '$.value') AS FLOAT64) AS cash_received,
        SAFE_CAST(JSON_VALUE(cells_spent, '$.value') AS FLOAT64) AS cash_spent,
        SAFE_CAST(JSON_VALUE(cells_close, '$.value') AS FLOAT64) AS closing_balance,
        JSON_VALUE(cells, '$.attributes[0].value') AS account_id,
        base.ingestion_time
    FROM 
        report_base base,
        UNNEST(JSON_QUERY_ARRAY(payload, '$.rows')) AS r
        CROSS JOIN UNNEST(JSON_QUERY_ARRAY(r, '$.rows')) AS row_item
        LEFT JOIN UNNEST(JSON_QUERY_ARRAY(row_item, '$.cells')) AS cells WITH OFFSET AS pos
        LEFT JOIN UNNEST(JSON_QUERY_ARRAY(row_item, '$.cells')) AS cells_open WITH OFFSET AS pos_open
        LEFT JOIN UNNEST(JSON_QUERY_ARRAY(row_item, '$.cells')) AS cells_received WITH OFFSET AS pos_received
        LEFT JOIN UNNEST(JSON_QUERY_ARRAY(row_item, '$.cells')) AS cells_spent WITH OFFSET AS pos_spent
        LEFT JOIN UNNEST(JSON_QUERY_ARRAY(row_item, '$.cells')) AS cells_close WITH OFFSET AS pos_close
    WHERE 
        pos = 0  -- bank account name
        AND pos_open = 1  -- opening balance
        AND pos_received = 2  -- cash received
        AND pos_spent = 3  -- cash spent
        AND pos_close = 4  -- closing balance
        AND JSON_VALUE(r, '$.row_type') = 'Section'
),

normalized_data AS (
    SELECT 
        report_id,
        report_date,
        report_name,
        report_type,
        updated_date_utc,
        report_titles,
        row_type,
        section_title,
        parent_section,
        level,
        row_label,
        opening_balance,
        cash_received,
        cash_spent,
        closing_balance,
        account_id,
        -- financial metrics (optional)
        SUM(CASE WHEN row_label = 'Total' THEN closing_balance END) 
            OVER (PARTITION BY report_id) AS total_closing_balance,
        -- metrics
        LAG(closing_balance) OVER (
            PARTITION BY report_id, row_label 
            ORDER BY report_date
        ) AS previous_closing_balance,
        CASE 
            WHEN LAG(closing_balance) OVER (
                PARTITION BY report_id, row_label 
                ORDER BY report_date
            ) != 0 
            THEN (closing_balance - LAG(closing_balance) OVER (
                PARTITION BY report_id, row_label 
                ORDER BY report_date
            )) / LAG(closing_balance) OVER (
                PARTITION BY report_id, row_label 
                ORDER BY report_date
            ) * 100
        END AS percentage_change,
        ingestion_time
    FROM 
        rows_parsed
    WHERE
        (row_label IS NOT NULL AND row_label != '')
        OR row_type = 'Section'
)

SELECT 
    report_id,
    report_date,
    report_name,
    report_type,
    updated_date_utc,
    report_titles,
    row_type,
    section_title,
    parent_section,
    level,
    row_label,
    opening_balance,
    cash_received,
    cash_spent,
    closing_balance,
    account_id,
    total_closing_balance,
    previous_closing_balance,
    percentage_change,
    ingestion_time
FROM normalized_data
ORDER BY 
    report_date DESC,
    level ASC,
    section_title ASC,
    row_label ASC