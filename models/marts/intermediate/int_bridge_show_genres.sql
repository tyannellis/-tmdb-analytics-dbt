with weekly_show_genres as (
    select * from {{ ref('stg_tmdb_project__genres') }}
),

distinct_show_genres as (
    select distinct
        tv_show_id,
        genre_id,
        genre_name
    from weekly_show_genres
)

select * from distinct_show_genres 