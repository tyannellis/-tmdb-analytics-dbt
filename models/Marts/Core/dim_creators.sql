with creators as (
    select * from {{ref("stg_tmdb_project__creators")}}
),

distinct_creators as (
    select distinct 
    creator_id,
    creator_name
    from creators
)

select * from distinct_creators