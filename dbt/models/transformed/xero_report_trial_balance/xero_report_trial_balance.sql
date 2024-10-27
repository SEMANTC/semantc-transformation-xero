{{ config(
    materialized='table',
    tags=['transformed', 'xero', 'reports', 'trial_balance']
) }}

WITH report_base AS (
    SELECT
        JSON_VALUE(payload, '$.report_id') AS report_id,
        JSON_VALUE(payload, '$.report_name') AS report_name,
        JSON_VALUE(payload, '$.report_type') AS report_type,
        PARSE_DATE('%d %B %Y', JSON_VALUE(payload, '$.report_date')) AS report_date,
        CAST(JSON_VALUE(payload, '$.updated_date_utc') AS TIMESTAMP) AS updated_date_utc,
        ARRAY(
            SELECT value 
            FROM UNNEST(JSON_QUERY_ARRAY(payload, '$.report_titles')) AS value
        ) AS report_titles,
        payload,
        ingestion_time
    FROM 
        {{ source('raw', 'xero_report_trial_balance') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY JSON_VALUE(payload, '$.report_id')
        ORDER BY ingestion_time DESC
    ) = 1
),

rows_parsed AS (
    -- Extract Sections and Rows
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
        CAST(NULL AS STRING) AS account_name,
        CAST(NULL AS STRING) AS account_id,
        CAST(NULL AS FLOAT64) AS debit,
        CAST(NULL AS FLOAT64) AS credit,
        CAST(NULL AS FLOAT64) AS ytd_debit,
        CAST(NULL AS FLOAT64) AS ytd_credit,
        base.ingestion_time
    FROM 
        report_base base,
        UNNEST(JSON_QUERY_ARRAY(base.payload, '$.rows')) AS r
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
        JSON_VALUE(row_item, '$.cells[0].value') AS account_name,
        JSON_VALUE(row_item, '$.cells[0].attributes[0].value') AS account_id,
        SAFE_CAST(NULLIF(JSON_VALUE(row_item, '$.cells[1].value'), '') AS FLOAT64) AS debit,
        SAFE_CAST(NULLIF(JSON_VALUE(row_item, '$.cells[2].value'), '') AS FLOAT64) AS credit,
        SAFE_CAST(NULLIF(JSON_VALUE(row_item, '$.cells[3].value'), '') AS FLOAT64) AS ytd_debit,
        SAFE_CAST(NULLIF(JSON_VALUE(row_item, '$.cells[4].value'), '') AS FLOAT64) AS ytd_credit,
        base.ingestion_time
    FROM 
        report_base base,
        UNNEST(JSON_QUERY_ARRAY(base.payload, '$.rows')) AS r,
        UNNEST(IFNULL(JSON_QUERY_ARRAY(r, '$.rows'), [])) AS row_item
    WHERE 
        JSON_VALUE(r, '$.row_type') = 'Section' 
        AND JSON_VALUE(row_item, '$.row_type') = 'Row'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY base.report_id, JSON_VALUE(r, '$.title'), JSON_VALUE(row_item, '$.cells[0].value')
        ORDER BY base.ingestion_time DESC
    ) = 1
),

summary_rows AS (
    -- Extract Summary Rows
    SELECT
        base.report_id,
        base.report_date,
        base.report_name,
        base.report_type,
        base.updated_date_utc,
        base.report_titles,
        JSON_VALUE(summary_row, '$.row_type') AS row_type,
        CAST(NULL AS STRING) AS section_title,
        CAST(NULL AS STRING) AS parent_section,
        2 AS level,
        JSON_VALUE(summary_row, '$.cells[0].value') AS account_name,
        CAST(NULL AS STRING) AS account_id,
        SAFE_CAST(NULLIF(JSON_VALUE(summary_row, '$.cells[1].value'), '') AS FLOAT64) AS debit,
        SAFE_CAST(NULLIF(JSON_VALUE(summary_row, '$.cells[2].value'), '') AS FLOAT64) AS credit,
        SAFE_CAST(NULLIF(JSON_VALUE(summary_row, '$.cells[3].value'), '') AS FLOAT64) AS ytd_debit,
        SAFE_CAST(NULLIF(JSON_VALUE(summary_row, '$.cells[4].value'), '') AS FLOAT64) AS ytd_credit,
        base.ingestion_time
    FROM 
        report_base base,
        UNNEST(JSON_QUERY_ARRAY(base.payload, '$.rows')) AS r,
        UNNEST(JSON_QUERY_ARRAY(r, '$.rows')) AS row_item,
        UNNEST(JSON_QUERY_ARRAY(row_item, '$.rows')) AS summary_row
    WHERE 
        JSON_VALUE(row_item, '$.row_type') = 'Section' 
        AND JSON_VALUE(summary_row, '$.row_type') = 'SummaryRow'
),

normalized_data AS (
    -- Combine Rows and Summary Rows
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
        account_name,
        account_id,
        debit,
        credit,
        ytd_debit,
        ytd_credit,
        ingestion_time
    FROM rows_parsed

    UNION ALL

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
        account_name,
        account_id,
        debit,
        credit,
        ytd_debit,
        ytd_credit,
        ingestion_time
    FROM summary_rows
),

final_data AS (
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
        account_name,
        account_id,
        debit,
        credit,
        ytd_debit,
        ytd_credit,
        ingestion_time
    FROM 
        normalized_data
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY report_id, section_title, account_name, account_id
        ORDER BY ingestion_time DESC
    ) = 1
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
    account_name,
    account_id,
    debit,
    credit,
    ytd_debit,
    ytd_credit,
    ingestion_time
FROM 
    final_data
ORDER BY 
    report_date DESC,
    level ASC,
    section_title ASC,
    account_name ASC