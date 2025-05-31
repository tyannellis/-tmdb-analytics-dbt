with weekly_show_networks_availability as (
select * from {{ref("stg_tmdb_project__networks")}}
),

distinct_weekly_show_networks_availability as (
select 
distinct 
tv_show_id,
network_id,
network_name,
start_of_week_date 
from weekly_show_networks_availability )


select * from distinct_weekly_show_networks_availability