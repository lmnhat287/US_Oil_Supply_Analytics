{{ config(materialized='view') }}

WITH source_data AS (
    -- dbt sẽ tự động hiểu source này trỏ đến file stg_oil_prices.parquet 
    -- nhờ vào cấu hình trong file sources.yml
    SELECT * FROM {{ source('data_lake', 'stg_oil_prices') }}
)

SELECT
    -- Đảm bảo định dạng ngày tháng chuẩn để dbt thực hiện các phép toán thời gian
    CAST(price_date AS DATE) AS price_date,
    
    -- Giá dầu WTI (Dùng kiểu DOUBLE cho DuckDB để tính toán chính xác)
    CAST(price_wti AS DOUBLE) AS price_wti,
    
    -- Ghi nhận thời điểm nạp dữ liệu
    CAST(ingestion_date AS DATE) AS ingestion_date

FROM source_data

-- Bạn có thể thêm điều kiện lọc dữ liệu lỗi nếu cần
WHERE price_wti IS NOT NULL