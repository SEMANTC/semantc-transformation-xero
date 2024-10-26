{{ config(
    tags=['transformed', 'xero', 'reports', 'profit_and_loss']
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
        {{ source('raw', 'xero_report_profit_and_loss') }}
),

rows_parsed AS (
    -- section headers (e.g., income, less cost of sales, less operating expenses)
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
        0 AS level,  -- assign level 0 for sections
        CAST(NULL AS STRING) AS row_label,
        CAST(NULL AS FLOAT64) AS amount,
        CAST(NULL AS STRING) AS attribute_id,
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
        JSON_VALUE(section_row, '$.row_type') AS row_type,
        JSON_VALUE(r, '$.title') AS section_title,
        JSON_VALUE(r, '$.title') AS parent_section,
        1 AS level,  -- assign level 1 for line items
        JSON_VALUE(cells, '$.value') AS row_label,
        SAFE_CAST(JSON_VALUE(cells_amount, '$.value') AS FLOAT64) AS amount,
        JSON_VALUE(cells, '$.attributes[0].id') AS attribute_id,
        JSON_VALUE(cells, '$.attributes[0].value') AS account_id,
        base.ingestion_time
    FROM 
        report_base base,
        UNNEST(JSON_QUERY_ARRAY(payload, '$.rows')) AS r
        CROSS JOIN UNNEST(JSON_QUERY_ARRAY(r, '$.rows')) AS section_row
        LEFT JOIN UNNEST(JSON_QUERY_ARRAY(section_row, '$.cells')) cells WITH OFFSET AS pos
        LEFT JOIN UNNEST(JSON_QUERY_ARRAY(section_row, '$.cells')) cells_amount WITH OFFSET AS pos_amount
    WHERE 
        pos = 0 AND pos_amount = 1
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
        amount,
        -- key financial metrics
        SUM(CASE WHEN row_label = 'Total Income' THEN amount END) 
            OVER (PARTITION BY report_id) AS total_income,
        SUM(CASE WHEN row_label = 'Total Cost of Sales' THEN amount END) 
            OVER (PARTITION BY report_id) AS total_cost_of_sales,
        SUM(CASE WHEN row_label = 'Gross Profit' THEN amount END) 
            OVER (PARTITION BY report_id) AS gross_profit,
        SUM(CASE WHEN row_label = 'Total Operating Expenses' THEN amount END) 
            OVER (PARTITION BY report_id) AS total_operating_expenses,
        SUM(CASE WHEN row_label = 'Net Profit' THEN amount END) 
            OVER (PARTITION BY report_id) AS net_profit,
        -- account tracking
        attribute_id,
        account_id,
        -- metrics
        LAG(amount) OVER (
            PARTITION BY report_id, row_label 
            ORDER BY report_date
        ) AS previous_amount,
        CASE 
            WHEN LAG(amount) OVER (
                PARTITION BY report_id, row_label 
                ORDER BY report_date
            ) != 0 
            THEN (amount - LAG(amount) OVER (
                PARTITION BY report_id, row_label 
                ORDER BY report_date
            )) / LAG(amount) OVER (
                PARTITION BY report_id, row_label 
                ORDER BY report_date
            ) * 100
        END AS percentage_change,
        ingestion_time
    FROM 
        rows_parsed
    WHERE
        -- include both sections and non-empty line items
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
    amount,
    total_income,
    total_cost_of_sales,
    gross_profit,
    total_operating_expenses,
    net_profit,
    attribute_id,
    account_id,
    previous_amount,
    percentage_change,
    ingestion_time
FROM normalized_data
ORDER BY 
    report_date DESC,
    level,
    section_title,
    row_label