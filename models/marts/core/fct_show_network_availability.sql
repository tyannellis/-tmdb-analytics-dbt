{{ config(
    materialized='incremental',
    unique_key=['tv_show_id', 'start_of_week_date']
) }}


with weekly_show_networks_availability as (
select 
tv_show_id,
network_id,
network_name,
start_of_week_date 
 from {{ref("stg_tmdb_project__networks")}}
),

distinct_weekly_show_networks_availability as (
select 
distinct 
tv_show_id,
network_id,
network_name,
start_of_week_date
from weekly_show_networks_availability 
where 
    {% if is_incremental() %}
        start_of_week_date > (select max(start_of_week_date) from {{ this }})
    {% else %}
        1=1
    {% endif %} 

)


select * from distinct_weekly_show_networks_availability