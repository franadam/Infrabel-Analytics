with source as (
    select * from {{ source('staging', 'punctuality_iceberg') }}
),

renamed as (
    select
        unique_row_id,
        filename,
        CAST(date_parse(nullif(datdep, '01JAN2050'), '%d%b%Y') AS date) as datdep,
        train_no,
        relation,
        train_serv,
        ptcar_no,
        thop1_cod,
        line_no_dep,
        real_time_arr,
        real_time_dep,
        planned_time_arr,
        planned_time_dep,
        delay_arr,
        delay_dep,
        circ_typ,
        relation_direction,
        ptcar_lg_nm_nl,
        line_no_arr,
        CAST(date_parse(nullif(planned_date_arr, '01JAN2050'), '%d%b%Y') AS date) as planned_date_arr,
        CAST(date_parse(nullif(planned_date_dep, '01JAN2050'), '%d%b%Y') AS date) as planned_date_dep,
        CAST(date_parse(nullif(real_date_arr, '01JAN2050'), '%d%b%Y') AS date) as real_date_arr,
        CAST(date_parse(nullif(real_date_dep, '01JAN2050'), '%d%b%Y') AS date) as real_date_dep
    from source
)

select * from renamed
