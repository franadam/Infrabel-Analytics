with source as (

    select * from {{ source('staging', 'station_ext') }}

),

renamed as (

    select
        uri,
        name,
        "alternative-fr",
        "alternative-nl",
        "alternative-de",
        "alternative-en",
        "taf-tap-code",
        "telegraph-code",
        upper("country-code") as country_code, 
        {{ get_full_country_name("country-code") }} as country,
        try_cast(longitude as DOUBLE) as longitude,
        try_cast(latitude as DOUBLE) as latitude,
        avg_stop_times,
        official_transfer_time

    from source
    where "country-code" not like ''  and latitude not like '' 
)

select * from renamed
