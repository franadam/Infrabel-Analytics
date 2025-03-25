with 

source as (

    select * from {{ source('staging', 'geography_ext') }}

),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['id', 'lat', 'lng']) }} as geo_id
        , REPLACE(city_ascii, '"', '') as city
        , CAST(REPLACE(lat, '"', '') AS DOUBLE) as latitude
        , CAST(REPLACE(lng, '"', '') AS DOUBLE) as longitude
        , REPLACE(admin_name, '"', '') as region
        , {{ get_capital_description('capital') }} as city_type
        , CAST(REPLACE(population, '"', '') AS INT) as population
    from source
    where country like '"Belgium"'
)

select * from renamed
