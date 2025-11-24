with imports as (
    select
        import_id as source_id,   -- LẤY ID GỐC
        report_date,
        year,
        month,
        origin_location as location_name,
        oil_grade,
        'Import' as supply_source,
        volume_bbl
    from {{ ref('stg_imports') }}
),

production as (
    select
        production_id as source_id, -- LẤY ID GỐC
        report_date,
        year,
        month,
        origin_location as location_name,
        oil_grade,
        'Domestic Production' as supply_source,
        volume_bbl
    from {{ ref('stg_production') }}
),

final_data as (
    select * from imports
    union all
    select * from production
)

select
    -- CÔNG THỨC ID MỚI: Dựa trên Nguồn + ID Gốc (Đảm bảo duy nhất 100%)
    md5(concat(supply_source, '-', cast(source_id as char))) as supply_id,
    
    report_date,
    year,
    month,
    
    -- Giữ nguyên fix lỗi font chữ (Collation) của tuần trước
    cast(location_name as char) collate utf8mb4_unicode_ci as location_name,
    cast(supply_source as char) collate utf8mb4_unicode_ci as supply_source,
    cast(oil_grade as char) collate utf8mb4_unicode_ci as oil_grade,
    
    volume_bbl

from final_data