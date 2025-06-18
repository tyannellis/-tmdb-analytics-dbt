{{ config(
    materialized = 'incremental',
    unique_key = 'surrogate_key',
    incremental_strategy = 'insert_overwrite',
    partition_by = { 'field': 'trending_week_start_date', 'data_type': 'date' }
) }}

with genre_base as
 (select 
 dim.genre_name,
 bridge.tv_show_id
from {{ ref('dim_genres') }} as dim
join {{ ref('int_bridge_show_genres') }} as bridge 
on dim.genre_id = bridge.genre_id),



trending_shows_with_dim as (select
fact.trending_week_start_date,
fact.tv_show_id,
fact.lifetime_vote_average,
fact.lifetime_vote_count,
dim.number_of_episodes,
dim.number_of_seasons
from {{ref("fct_trending_tv_shows")}} as fact
join {{ref("dim_tv_shows")}} as dim
on fact.tv_show_id = dim.tv_show_id
where dim.is_current = true
{% if is_incremental() %}
  and fact.trending_week_start_date > (
    select max(trending_week_start_date)
    from {{ this }}
  )
{% endif %})

Select
trending_week_start_date,
genre_name,
{{ dbt_utils.generate_surrogate_key([
    'trending_week_start_date',
    'genre_name'
]) }} as surrogate_key,
count(tv_show.tv_show_id) as genre_show_count,
round(avg(lifetime_vote_average),2) as genre_lifetime_vote_average,
sum(lifetime_vote_count)as  genre_lifetime_vote_count,
round(avg(number_of_episodes),0) as genre_number_of_episodes_avg,
round(avg(number_of_seasons),0) as genre_number_of_seasons_avg
from genre_base as genre
join trending_shows_with_dim as tv_show 
on genre.tv_show_id = tv_show.tv_show_id
group by 
trending_week_start_date,
genre_name





