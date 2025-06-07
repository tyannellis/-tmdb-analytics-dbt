with networks as (
    select 
network_id,
network_name
from {{ref("stg_tmdb_project__networks")}}
),

distinct_networks as (
select distinct
network_id,
network_name
from networks 

)

select * from distinct_networks