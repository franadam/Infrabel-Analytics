{{ config(materialized='table') }}

SELECT DISTINCT 
    {{ dbt_utils.generate_surrogate_key(['ptcar_lg_nm_nl', 'PTCAR_NO']) }} as pk_dim_station
    , lower(trim(ptcar_lg_nm_nl)) as name
    , PTCAR_NO AS measuring_point
FROM {{ref('stg_staging__punctuality_iceberg')}}
