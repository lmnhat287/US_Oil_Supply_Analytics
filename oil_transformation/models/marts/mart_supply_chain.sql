{{ config(materialized='table') }}

WITH imports AS (
    SELECT 
        date_trunc('month', ingestion_date) AS report_month,
        SUM(quantity_thousand_bbl) AS total_imported_bbl
    FROM {{ ref('stg_imports') }}
    GROUP BY 1
),

production AS (
    SELECT 
        date_trunc('month', production_date) AS report_month,
        SUM(volume) AS total_production_bbl
    FROM {{ ref('stg_production') }}
    GROUP BY 1
),

-- Nếu bạn chưa tạo file stg_oil_exports.sql trong thư mục staging, 
-- thì mới dùng tạm hàm source đọc thẳng từ Parquet như bên dưới
exports AS (
    SELECT 
        date_trunc('month', date) AS report_month,
        SUM(value) AS total_exported_bbl
    FROM {{ source('data_lake', 'stg_oil_exports') }}
    GROUP BY 1
)

SELECT 
    COALESCE(p.report_month, i.report_month, e.report_month) AS report_month,
    COALESCE(p.total_production_bbl, 0) AS total_production_bbl,
    COALESCE(i.total_imported_bbl, 0) AS total_imported_bbl,
    COALESCE(e.total_exported_bbl, 0) AS total_exported_bbl,
    -- Tính toán cán cân cung cầu cơ bản
    (COALESCE(p.total_production_bbl, 0) + COALESCE(i.total_imported_bbl, 0) - COALESCE(e.total_exported_bbl, 0)) AS net_supply_bbl
FROM production p
FULL OUTER JOIN imports i ON p.report_month = i.report_month
FULL OUTER JOIN exports e ON p.report_month = e.report_month
ORDER BY report_month DESC