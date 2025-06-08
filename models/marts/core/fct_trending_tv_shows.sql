{{ config(
    materialized='incremental',
    unique_key=['tv_show_id', 'trending_week_start_date']
) }}



with base as (

   select 
    start_of_week_date,
    tv_show_id,
    lifetime_vote_average,
    number_of_episodes,
    lifetime_vote_count
   from {{ref("stg_tmdb_project__trending_tv_shows")}} 
)

select 
    start_of_week_date as trending_week_start_date,
    tv_show_id,
    lifetime_vote_average,
    number_of_episodes,
    lifetime_vote_count
    from base
    where 
    {% if is_incremental() %}
        start_of_week_date > (select max(trending_week_start_date) from {{ this }})
    {% else %}
        1=1
    {% endif %} 

