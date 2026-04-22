{{ config(materialized='table') }}

WITH imports AS (
    SELECT 
        -- Nạp thêm destination_name và grade_name vào hàm băm
        md5(concat(year, '-', month, '-', origin_name, '-', destination_name, '-', grade_name)) AS source_id,
        'Import' AS supply_source,
        make_date(CAST(year AS INTEGER), CAST(month AS INTEGER), 1) AS supply_date,
        quantity_thousand_bbl AS volume_bbl
    FROM {{ ref('stg_imports') }}
    WHERE quantity_thousand_bbl IS NOT NULL
),

production AS (
    SELECT 
        -- Nạp thêm county, commodity và land_class vào hàm băm
        md5(concat(production_date, '-', state, '-', COALESCE(county, 'none'), '-', commodity, '-', land_class)) AS source_id,
        'Domestic_Production' AS supply_source,
        production_date AS supply_date,
        volume AS volume_bbl
    FROM {{ ref('stg_production') }}
    WHERE volume IS NOT NULL
)

SELECT * FROM imports
UNION ALL
SELECT * FROM production