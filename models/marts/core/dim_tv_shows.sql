with tv_show_source as (

select * from {{ref("stg_tmdb_project__trending_tv_shows")}}

),

distinct_tv_shows as (

select 
distinct 
tv_show_id,
first_episode_air_date,
tv_show_type,
tv_show_status,
original_language_code,
original_name,
number_of_seasons
from tv_show_source
)

select * from distinct_tv_shows