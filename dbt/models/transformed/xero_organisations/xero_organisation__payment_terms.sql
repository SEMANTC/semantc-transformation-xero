{{ config(
    tags=['transformed', 'xero', 'organisations', 'payment_terms']
) }}

SELECT DISTINCT
    JSON_VALUE(payload, '$.organisation_id') AS organisation_id,
    JSON_VALUE(payment_term, '$.term_type') AS term_type,
    JSON_VALUE(payment_term, '$.value') AS value,
    ingestion_time
FROM 
    {{ source('raw', 'xero_organisations') }},
    UNNEST(JSON_QUERY_ARRAY(payload, '$.payment_terms')) AS payment_term
WHERE 
    JSON_VALUE(payment_term, '$.term_type') IS NOT NULL