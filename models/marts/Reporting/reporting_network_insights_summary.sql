{{ config(
    materialized = 'incremental',
    unique_key = 'surrogate_key',
    incremental_strategy = 'insert_overwrite',
    partition_by = { 'field': 'trending_week_start_date', 'data_type': 'date' }
) }}

with trending_shows_and_networks as 
(select 
    network_name,
    network_id,
    trending_week_start_date,
    lifetime_vote_average,
    lifetime_vote_count,
    trending.tv_show_id,
    number_of_episodes,
    number_of_seasons,
    tv_show_status,
    tv_show_type,
    original_name,
    row_number () over(partition by network_id, trending_week_start_date order by lifetime_vote_count, lifetime_vote_average) as voting_rank
  from {{ ref("fct_show_network_availability") }} as network
  join {{ ref("fct_trending_tv_shows") }} as trending
    on trending.tv_show_id = network.tv_show_id
    and start_of_week_date = trending_week_start_date
  join {{ ref("dim_tv_shows") }} as dims
    on network.tv_show_id = dims.tv_show_id
    and dims.is_current = true
),



top_interacted_show_per_network as (
select 
network_name,
trending_week_start_date,
original_name as network_most_popular_show_name,
trending_shows_and_networks.tv_show_id as network_most_popular_show_id
from trending_shows_and_networks
where voting_rank =  1
)



select 
trending_shows_and_networks.network_name,
trending_shows_and_networks.trending_week_start_date,
round(avg(lifetime_vote_average),2) as network_weekly_vote_average,
sum(lifetime_vote_count) as  network_vote_count,
round(avg(number_of_episodes),0) as average_episode_per_show ,
round(avg(number_of_seasons),0) as average_seasons_per_show,
count(trending_shows_and_networks.tv_show_id) as total_tv_show_per_network,
network_most_popular_show_name,
network_most_popular_show_id,
{{ dbt_utils.generate_surrogate_key([
  'trending_shows_and_networks.trending_week_start_date',
  'trending_shows_and_networks.network_name'
]) }} as surrogate_key
from trending_shows_and_networks
left join top_interacted_show_per_network
on trending_shows_and_networks.network_name = top_interacted_show_per_network.network_name
and trending_shows_and_networks.trending_week_start_date = top_interacted_show_per_network.trending_week_start_date 
{% if is_incremental() %}
        where trending_shows_and_networks.trending_week_start_date > (
            coalesce(max(trending_week_start_date),  '2000-01-01')
            from {{ this }}
        )
    {% endif %}

group by 
trending_shows_and_networks.network_name,
trending_shows_and_networks.trending_week_start_date,
network_most_popular_show_name,
network_most_popular_show_id




