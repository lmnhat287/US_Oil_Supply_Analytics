{{ config(materialized='table') }}

WITH prices AS (
    SELECT 
        -- Chuyển ngày cụ thể về ngày đầu tháng (Ví dụ: 2023-01-15 -> 2023-01-01)
        DATE_FORMAT(price_date, '%Y-%m-01') as month,
        AVG(price_wti) as avg_price_wti
    FROM {{ source('oil_dw', 'stg_oil_prices') }}
    GROUP BY 1
),

stocks AS (
    SELECT 
        DATE_FORMAT(`date`, '%Y-%m-01') as month,
        -- Tồn kho lấy trung bình các tuần trong tháng là hợp lý nhất
        AVG(value) as avg_stocks_qty
    FROM {{ source('oil_dw', 'stg_oil_stocks') }}
    GROUP BY 1
),

refinery AS (
    SELECT 
        DATE_FORMAT(`date`, '%Y-%m-01') as month,
        -- Công suất lọc dầu lấy trung bình
        AVG(value) as avg_refinery_input_qty
    FROM {{ source('oil_dw', 'stg_refinery_inputs') }}
    GROUP BY 1
),

exports AS (
    SELECT 
        DATE_FORMAT(`date`, '%Y-%m-01') as month,
        -- Xuất khẩu vốn dĩ đã là monthly rồi, nhưng group by cho chắc
        MAX(value) as total_export_qty
    FROM {{ source('oil_dw', 'stg_oil_exports') }}
    GROUP BY 1
)

SELECT 
    -- Chuyển lại string thành dạng Date để Power BI hiểu là trục thời gian
    STR_TO_DATE(p.month, '%Y-%m-%d') as `month`,
    ROUND(p.avg_price_wti, 2) as price_wti,
    ROUND(s.avg_stocks_qty, 0) as stocks_qty,
    ROUND(r.avg_refinery_input_qty, 0) as refinery_input_qty,
    ROUND(e.total_export_qty, 0) as export_qty
FROM prices p
LEFT JOIN stocks s ON p.month = s.month
LEFT JOIN refinery r ON p.month = r.month
LEFT JOIN exports e ON p.month = e.month
WHERE p.month >= '2020-01-01'
ORDER BY p.month DESC