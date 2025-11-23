with imports as (
    select
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
    -- Tạo ID bằng cách nối chuỗi (Dùng COALESCE để tránh lỗi nếu có cột bị Null)
    md5(concat(
        coalesce(supply_source, ''), 
        coalesce(location_name, ''), 
        coalesce(cast(report_date as char), ''), 
        coalesce(oil_grade, ''), 
        coalesce(cast(volume_bbl as char), '')
    )) as supply_id,
    
    -- Liệt kê rõ các cột thay vì dùng *
    report_date,
    year,
    month,
    location_name,
    supply_source,
    oil_grade,
    volume_bbl

from final_data