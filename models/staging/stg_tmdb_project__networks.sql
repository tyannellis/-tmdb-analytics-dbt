with source as (
    select * from {{source('TMDB_PROJECT','networks')}}

),

renamed as (
select
tv_show_id,
network_id,
network_name,
load_date,
WEEK_START_DATE as start_of_week_date
from source

)

select * from renamed

