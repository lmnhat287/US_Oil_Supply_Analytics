with source as (
    -- Dùng hàm source() để dbt tự tìm bảng, không cần gõ tên cứng
    select * from {{ source('oil_raw', 'stg_crude_oil_imports') }}
),

renamed as (
    select
        -- Tạo ID duy nhất (nếu bảng gốc chưa có thì dùng hàm băm, ở đây bảng gốc đã có id)
        id as import_id,
        year,
        month,
        
        -- Chuẩn hóa ngày báo cáo về ngày đầu tháng (ví dụ: 2023-05-01) để dễ join
        str_to_date(concat(year, '-', month, '-01'), '%Y-%m-%d') as report_date,
        
        origin_name as origin_location,
        origin_type,
        destination_name,
        grade_name as oil_grade,
        
        -- LOGIC QUAN TRỌNG: Quy đổi đơn vị (Nghìn thùng -> Thùng)
        quantity_thousand_bbl as volume_bbl,
        
        ingestion_date
    from source
)

select * from renamed