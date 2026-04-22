{{ config(materialized='view') }}

SELECT * FROM {{ source('data_lake', 'stg_federal_production') }}