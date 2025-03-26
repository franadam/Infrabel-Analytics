{{ config(materialized='table') }}
with
    direction_tb as (
            select
                split_part(RELATION_DIRECTION, ':', 2) AS direction
                , PTCAR_NO AS measuring_point
            FROM {{ref('stg_staging__punctuality_iceberg')}}
    ), 
    station_dep as (
        select
            split_part(direction, ' -> ', 1) AS station,
            measuring_point
        from direction_tb
    ),    
    station_arr as (
        SELECT
            split_part(direction, ' -> ', 2) AS station,
            measuring_point
        from direction_tb
    ),
    station_unioned as (
        select *
        from station_arr
        union all
        select *
        from station_dep
    )
SELECT DISTINCT 
    station as name
    , measuring_point
FROM station_unioned
where station is not null
