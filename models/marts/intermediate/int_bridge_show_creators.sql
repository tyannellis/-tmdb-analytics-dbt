with show_creators as (
    select
    tv_show_id,
    creator_id
     from {{ref("stg_tmdb_project__creators")}}
),

distinct_show_creators as(

    select distinct 
    tv_show_id,
    creator_id
    from show_creators
)

select*from distinct_show_creators