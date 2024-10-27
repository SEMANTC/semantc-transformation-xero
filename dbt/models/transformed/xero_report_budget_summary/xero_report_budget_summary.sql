-- models/transformed/xero_report_budget_summary.sql

{{ config(
    materialized='table',
    tags=['transformed', 'xero', 'reports', 'budget_summary']
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
        {{ source('raw', 'xero_report_budsummary') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY JSON_VALUE(payload, '$.report_id')
        ORDER BY ingestion_time DESC
    ) = 1
),

header AS (
    -- Extract the header row to get the months
    SELECT ARRAY(
            SELECT JSON_VALUE(c, '$.value') 
            FROM UNNEST(JSON_QUERY_ARRAY(r, '$.cells')) AS c 
            WHERE JSON_VALUE(c, '$.value') != 'Account'
        ) AS months
    FROM 
        report_base,
        UNNEST(JSON_QUERY_ARRAY(payload, '$.rows')) AS r
    WHERE 
        JSON_VALUE(r, '$.row_type') = 'Header'
    LIMIT 1
),

rows_parsed AS (
    -- Extract Sections and their Rows
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
        CAST(NULL AS STRING) AS account_id,
        CAST(NULL AS ARRAY<STRING>) AS amounts,
        base.ingestion_time
    FROM 
        report_base base,
        UNNEST(JSON_QUERY_ARRAY(payload, '$.rows')) AS r
    WHERE 
        JSON_VALUE(r, '$.row_type') = 'Section'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY base.report_id, JSON_VALUE(r, '$.title')
        ORDER BY base.ingestion_time DESC
    ) = 1

    UNION ALL

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
        JSON_VALUE(row_item, '$.cells[0].value') AS row_label,
        JSON_VALUE(row_item, '$.cells[0].attributes[0].value') AS account_id,
        ARRAY(
            SELECT JSON_VALUE(c, '$.value')
            FROM UNNEST(JSON_QUERY_ARRAY(row_item, '$.cells')) AS c WITH OFFSET pos
            WHERE pos > 0  -- Skip the first cell ("Account")
            ORDER BY pos
        ) AS amounts,
        base.ingestion_time
    FROM 
        report_base base,
        UNNEST(JSON_QUERY_ARRAY(payload, '$.rows')) AS r,
        UNNEST(IFNULL(JSON_QUERY_ARRAY(r, '$.rows'), [])) AS row_item
    WHERE 
        JSON_VALUE(r, '$.row_type') = 'Section' 
        AND JSON_VALUE(row_item, '$.row_type') = 'Row'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY base.report_id, JSON_VALUE(r, '$.title'), JSON_VALUE(row_item, '$.cells[0].value')
        ORDER BY base.ingestion_time DESC
    ) = 1
),

normalized_data AS (

    -- Handle Section Rows Without Cross Joining Months
    SELECT
        rp.report_id,
        rp.report_date,
        rp.report_name,
        rp.report_type,
        rp.updated_date_utc,
        rp.report_titles,
        rp.row_type,
        rp.section_title,
        rp.parent_section,
        rp.level,
        rp.row_label,
        rp.account_id,
        NULL AS month,
        NULL AS amount,
        rp.ingestion_time
    FROM 
        rows_parsed rp
    WHERE 
        rp.row_type = 'Section'

    UNION ALL

    -- Handle Row Entries with Cross Joining Months
    SELECT
        rp.report_id,
        rp.report_date,
        rp.report_name,
        rp.report_type,
        rp.updated_date_utc,
        rp.report_titles,
        rp.row_type,
        rp.section_title,
        rp.parent_section,
        rp.level,
        rp.row_label,
        rp.account_id,
        m AS month,
        SAFE_CAST(rp.amounts[OFFSET(pos)] AS FLOAT64) AS amount,
        rp.ingestion_time
    FROM 
        rows_parsed rp
    CROSS JOIN 
        header h
    CROSS JOIN 
        UNNEST(h.months) AS m WITH OFFSET pos
    WHERE 
        rp.row_type = 'Row'
        AND rp.row_label IS NOT NULL 
        AND rp.row_label != ''

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
    account_id,
    month,
    amount,
    ingestion_time
FROM 
    normalized_data
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY report_id, section_title, row_label, month
    ORDER BY ingestion_time DESC
) = 1
ORDER BY 
    report_date DESC,
    level ASC,
    section_title ASC,
    row_label ASC,
    month ASC