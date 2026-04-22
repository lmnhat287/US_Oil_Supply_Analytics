{{ config(materialized='view') }}

SELECT 
    year,
    month,
    origin_name,
    origin_type,
    destination_name,
    grade_name,
    quantity_thousand_bbl,
    ingestion_date
FROM {{ source('data_lake', 'stg_crude_oil_imports') }}