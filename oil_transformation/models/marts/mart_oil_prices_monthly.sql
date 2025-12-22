with daily_prices as (
    select * from {{ ref('stg_prices') }}
),

monthly_avg as (
    select
        year,
        month,
        -- Tạo ID tháng (ví dụ: 2023-01-01) để Metabase dễ vẽ biểu đồ
        str_to_date(concat(year, '-', month, '-01'), '%Y-%m-%d') as report_date,
        
        -- Tính giá trung bình, làm tròn 2 chữ số
        round(avg(price_wti), 2) as avg_price_wti,
        
        -- Đếm số ngày giao dịch trong tháng (để kiểm tra data)
        count(*) as trading_days
        
    from daily_prices
    group by year, month, report_date
)

select * from monthly_avg
order by year desc, month desc