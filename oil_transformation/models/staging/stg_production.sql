with source as (
    select * from {{ source('oil_raw', 'stg_federal_production') }}
),

filtered as (
    select
        id as production_id,
        production_date as report_date,
        year(production_date) as year,
        month(production_date) as month,
        
        -- Logic lấy địa điểm: Nếu bang null thì lấy vùng biển offshore
        coalesce(state, offshore_region) as origin_location,
        
        -- Production không phân loại phẩm cấp dầu chi tiết như Import
        'Unknown/Mixed' as oil_grade,
        
        volume as volume_bbl,
        commodity,
        disposition_code
        
    from source
    where 
        -- LOGIC QUAN TRỌNG: Chỉ lấy Dầu và Chỉ lấy lượng Bán ra
        commodity = 'Oil (bbl)'
        and disposition_code in ('01', '1')
        and volume > 0
)

select 
    production_id,
    report_date,
    year,
    month,
    origin_location,
    oil_grade,
    volume_bbl
from filtered