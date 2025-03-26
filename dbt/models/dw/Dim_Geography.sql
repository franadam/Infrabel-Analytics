{{ config(materialized='table') }}

select * 
from {{ref('stg_staging__geography_ext')}}