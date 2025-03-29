with 

source as (

    select * from {{ source('staging', 'stop_ext') }}

),

renamed as (

    select
        uri,
        parent_stop,
        longitude,
        latitude,
        name,
        alternative-fr,
        alternative-nl,
        alternative-de,
        alternative-en,
        platform

    from source

)

select * from renamed
