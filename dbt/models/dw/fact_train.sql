{{ config(materialized='table') }}

WITH safe_cast AS (
  SELECT         
    train_no AS FK_Dim_Train,
    datdep,
    planned_date_arr,
    planned_date_dep,
    real_date_arr,
    real_date_dep,
    try_cast(planned_time_arr AS time) AS planned_time_arr,
    try_cast(planned_time_dep AS time) AS planned_time_dep,
    try_cast(real_time_arr AS time) AS real_time_arr,
    try_cast(real_time_dep AS time) AS real_time_dep,
    delay_arr,
    delay_dep
  FROM {{ ref('stg_staging__punctuality_iceberg') }}
)
SELECT         
    FK_Dim_Train,
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
    CAST(hour(planned_time_arr) * 3600 + minute(planned_time_arr) * 60 + second(planned_time_arr) AS integer) AS TimeKey_planned_time_arr,
    CAST(hour(planned_time_dep) * 3600 + minute(planned_time_dep) * 60 + second(planned_time_dep) AS integer) AS TimeKey_planned_time_dep,
    CAST(hour(real_time_arr) * 3600 + minute(real_time_arr) * 60 + second(real_time_arr) AS integer) AS TimeKey_real_time_arr,
    CAST(hour(real_time_dep) * 3600 + minute(real_time_dep) * 60 + second(real_time_dep) AS integer) AS TimeKey_real_time_dep,
    delay_arr,
    delay_dep
FROM safe_cast

