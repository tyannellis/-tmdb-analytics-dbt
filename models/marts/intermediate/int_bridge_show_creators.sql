with show_creators as (
    select * from {{ref("stg_tmdb_project__creators")}}
),

distinct_show_creators as(

    select distinct 
    tv_show_id,
    creator_id,
    creator_name
    from show_creators
)

select*from distinct_show_creators