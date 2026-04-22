{{ config(materialized='table') }}

SELECT 
    EXTRACT(YEAR FROM price_date) AS price_year,
    EXTRACT(MONTH FROM price_date) AS price_month,
    date_trunc('month', price_date) AS report_month,
    ROUND(AVG(price_wti), 2) AS avg_wti_price
FROM {{ ref('stg_prices') }}
WHERE price_wti IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 3 DESC