with source as (
    select * from {{source('TMDB_PROJECT','spoken_language')}}

),

renamed as (
    select 
    tv_show_id,
    Language_name,
    load_date,
    WEEK_START_DATE as start_of_week_date
    from source

)

select * from renamed 