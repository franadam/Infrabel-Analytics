{{ config(materialized='table') }}

SELECT DISTINCT 
    {{ dbt_utils.generate_surrogate_key(['name', 'longitude', 'latitude']) }} as pk_dim_station
    , upper(trim(name)) as name
    , longitude
    , latitude
    , country_code
    , country as country_name
    , "telegraph-code" as telegraph_code
    , "taf-tap-code" as taf_tap_code
FROM {{ref('stg_staging__station_ext')}}
