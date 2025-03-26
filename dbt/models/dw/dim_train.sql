{{ config(materialized='table') }}

SELECT DISTINCT 
    TRAIN_NO  AS train_no
    , TRAIN_SERV AS operator
    , split_part(RELATION, ' ', 1) AS train_type
    , split_part(RELATION_DIRECTION, ':', 2) AS direction
FROM {{ref('stg_staging__punctuality_iceberg')}}
