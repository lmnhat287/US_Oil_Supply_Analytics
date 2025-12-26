with source as (
    select * from {{ source('oil_dw', 'stg_oil_prices') }}
),

renamed as (
    select
        id as price_id,
        price_date,
        -- Tách tháng năm để tý nữa gom nhóm
        year(price_date) as year,
        month(price_date) as month,
        price_wti,
        ingestion_date
    from source
)

select * from renamed