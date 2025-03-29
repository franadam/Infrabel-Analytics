{{ config(materialized='table') }}

WITH
    unique_train as (
        SELECT DISTINCT 
            train_no as unique_no
        FROM {{ref('stg_staging__punctuality_iceberg')}}
    )
SELECT  DISTINCT
   {{ dbt_utils.generate_surrogate_key(['unique_no', 'relation', 'TRAIN_SERV'])}} AS PK_Dim_Train
   , unique_no as train_no
    , TRAIN_SERV AS operator
    , RELATION AS relation
    , split_part(RELATION, ' ', 1) AS train_type
FROM {{ref('stg_staging__punctuality_iceberg')}} S
inner join unique_train on S.train_no = unique_no
  
