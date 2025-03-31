{{ 
    config(
        materialized='table',
        partition_by={
          "field": "FK_Dim_Date_dep",
          "data_type": "int",
          "granularity": "day"
        }
    ) 
}}

with
    safe_cast AS (
  SELECT 
    try_cast(S.TRAIN_NO as varchar) as train_no_string,
    try_cast(S.PTCAR_NO as varchar) as PTCAR_NO_string,
    try_cast(S.datdep as varchar) as datdep_string,
    try_cast(S.real_time_dep as varchar) as real_time_dep_string,
    T.PK_Dim_Train AS FK_Dim_Train,
    datdep,
    planned_date_arr,
    planned_date_dep,
    real_date_arr,
    real_date_dep,
    try_cast(planned_time_arr AS time) AS planned_time_arr,
    try_cast(planned_time_dep AS time) AS planned_time_dep,
    try_cast(real_time_arr AS time) AS real_time_arr,
    try_cast(real_time_dep AS time) AS real_time_dep,
    A.pk_dim_station as fk_dim_station,
    S.RELATION_DIRECTION,
    delay_arr,
    delay_dep,
    try_cast(S.delay_arr as varchar) as delay_arr_string,
    try_cast(S.delay_dep as varchar) as delay_dep_string,
    try_cast(planned_date_arr AS varchar) AS planned_date_arr_string,
    try_cast(planned_date_dep AS varchar) AS planned_date_dep_string,
    try_cast(real_date_arr AS varchar) AS real_date_arr_string,
    try_cast(real_date_dep AS varchar) AS real_date_dep_string,
    TRAIN_SERV,
    ptcar_lg_nm_nl
  FROM {{ ref('stg_staging__punctuality_iceberg') }} S
  left join {{ ref('dim_station') }} A on A.name = trim(ptcar_lg_nm_nl)
  left join {{ ref('dim_train') }} T on T.train_no = S.train_no
)
SELECT  
    {{ dbt_utils.generate_surrogate_key(
        [
        'train_no_string'
        , 'datdep_string'
        , 'PTCAR_NO_string'
        , 'TRAIN_SERV'
        , 'ptcar_lg_nm_nl'
        , 'real_time_dep_string'
        ])
    }}  as PK_Fact_Train,        
    FK_Dim_Train,
    fk_dim_station,
    CAST(
        year(try_cast(datdep AS date)) * 10000 +
        month(try_cast(datdep AS date)) * 100 +
        day(try_cast(datdep AS date)) AS integer
    ) AS FK_Dim_Date_dep,
    CAST(
        year(try_cast(planned_date_arr AS date)) * 10000 +
        month(try_cast(planned_date_arr AS date)) * 100 +
        day(try_cast(planned_date_arr AS date)) AS integer
    ) AS FK_Dim_Date_planned_arr,
    CAST(
        year(try_cast(planned_date_dep AS date)) * 10000 +
        month(try_cast(planned_date_dep AS date)) * 100 +
        day(try_cast(planned_date_dep AS date)) AS integer
    ) AS FK_Dim_Date_planned_dep,
    CAST(
        year(try_cast(real_date_arr AS date)) * 10000 +
        month(try_cast(real_date_arr AS date)) * 100 +
        day(try_cast(real_date_arr AS date)) AS integer
    ) AS FK_Dim_Date_real_arr,
    CAST(
        year(try_cast(real_date_dep AS date)) * 10000 +
        month(try_cast(real_date_dep AS date)) * 100 +
        day(try_cast(real_date_dep AS date)) AS integer
    ) AS FK_Dim_Date_real_dep,
    CAST(hour(planned_time_arr) * 3600 + minute(planned_time_arr) * 60 + second(planned_time_arr) AS integer) AS FK_Dim_Tim_planned_time_arr,
    CAST(hour(planned_time_dep) * 3600 + minute(planned_time_dep) * 60 + second(planned_time_dep) AS integer) AS FK_Dim_Tim_planned_time_dep,
    CAST(hour(real_time_arr) * 3600 + minute(real_time_arr) * 60 + second(real_time_arr) AS integer) AS FK_Dim_Tim_real_time_arr,
    CAST(hour(real_time_dep) * 3600 + minute(real_time_dep) * 60 + second(real_time_dep) AS integer) AS FK_Dim_Tim_real_time_dep,
    delay_arr,
    delay_dep
FROM safe_cast
where planned_time_dep is not null and planned_time_arr is not null and real_time_dep is not null and real_time_arr is not null
