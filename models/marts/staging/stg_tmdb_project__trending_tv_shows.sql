with source as (

select * from {{source("TMDB_PROJECT","tv_show")}}

),


renamed as (
select
tv_show_id,
first_air_date as first_episode_air_date,
tv_show_status,
tv_show_type,
vote_average as lifetime_vote_average,
original_language as original_language_code,
original_name, 
localized_name,
number_of_episodes,
number_of_seasons,
vote_count as lifetime_vote_count,
load_date,
start_of_week as start_of_week_date
from source 
)

select * from renamed

