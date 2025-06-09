-- 1 row per TV show per week

{{ config(
    materialized = 'incremental',
    unique_key = 'surrogate_key',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'trending_week_start_date', 'data_type': 'date'}
) }}

with trending_tv_show_base as (
select 
trending_week_start_date,
tv_show_id,
lifetime_vote_average,
lifetime_vote_count,
{{ dbt_utils.generate_surrogate_key([
  'tv_show_id',
  'trending_week_start_date'
]) }} as surrogate_key
from {{ref("fct_trending_tv_shows")}}
 {% if is_incremental() %}
        where trending_week_start_date > (
            select max(trending_week_start_date)
            from {{ this }}
        )
    {% endif %}
),

current_tv_dim as (select 
tv_show_id,
original_name,
number_of_episodes,
number_of_seasons,
first_episode_air_date,
original_language_code,
tv_show_status,
tv_show_type
from {{ref("dim_tv_shows")}}
where is_current = true)

select 
trending.trending_week_start_date,
trending.tv_show_id,
trending.lifetime_vote_average,
trending.lifetime_vote_count,
trending.surrogate_key,
dim.original_name,
dim.number_of_episodes,
dim.number_of_seasons,
dim.first_episode_air_date,
dim.original_language_code,
dim.tv_show_status,
dim.tv_show_type
from trending_tv_show_base as trending
inner join current_tv_dim as dim 
on trending.tv_show_id = dim.tv_show_id
