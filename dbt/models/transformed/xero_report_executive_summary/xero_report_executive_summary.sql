{{ config(
    tags=['transformed', 'xero', 'reports', 'executive_summary']
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
        {{ source('raw', 'xero_report_executive_summary') }}
),

rows_parsed AS (
    -- Extract Section Headers
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
        CAST(NULL AS FLOAT64) AS current_value,
        CAST(NULL AS FLOAT64) AS previous_value,
        CAST(NULL AS FLOAT64) AS variance,
        base.ingestion_time
    FROM 
        report_base base,
        UNNEST(JSON_QUERY_ARRAY(payload, '$.rows')) AS r
    WHERE 
        JSON_VALUE(r, '$.row_type') = 'Section'

    UNION ALL

    -- extract line items within sections
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
        SAFE_CAST(JSON_VALUE(row_item, '$.cells[1].value') AS FLOAT64) AS current_value,
        SAFE_CAST(JSON_VALUE(row_item, '$.cells[2].value') AS FLOAT64) AS previous_value,
        SAFE_CAST(REGEXP_REPLACE(JSON_VALUE(row_item, '$.cells[3].value'), '%', '') AS FLOAT64) AS variance,
        base.ingestion_time
    FROM 
        report_base base,
        UNNEST(JSON_QUERY_ARRAY(payload, '$.rows')) AS r,
        -- safely handle 'rows' which could be null or empty
        UNNEST(IFNULL(JSON_QUERY_ARRAY(r, '$.rows'), [])) AS row_item
    WHERE 
        JSON_VALUE(r, '$.row_type') = 'Section' 
        AND JSON_VALUE(row_item, '$.row_type') = 'Row'
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
        current_value,
        previous_value,
        variance,
        -- calculate variance if previous_value exists and is not zero
        CASE 
            WHEN previous_value IS NOT NULL AND previous_value != 0 THEN 
                (current_value - previous_value) / previous_value * 100
            ELSE NULL
        END AS calculated_variance,
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
    current_value,
    previous_value,
    variance,
    calculated_variance,
    ingestion_time
FROM normalized_data
ORDER BY 
    report_date DESC,
    level ASC,
    section_title ASC,
    row_label ASC