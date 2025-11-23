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
    -- Tạo ID
    md5(concat(
        coalesce(supply_source, ''), 
        coalesce(location_name, ''), 
        coalesce(cast(report_date as char), ''), 
        coalesce(oil_grade, ''), 
        coalesce(cast(volume_bbl as char), '')
    )) as supply_id,
    
    report_date,
    year,
    month,
    
    -- === FIX LỖI COLLATION===
    -- Ép kiểu về utf8mb4_unicode_ci để khớp với Metabase
    cast(location_name as char) collate utf8mb4_unicode_ci as location_name,
    cast(supply_source as char) collate utf8mb4_unicode_ci as supply_source,
    cast(oil_grade as char) collate utf8mb4_unicode_ci as oil_grade,
    
    volume_bbl

from final_data