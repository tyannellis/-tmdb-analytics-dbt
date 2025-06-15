with source as (
    select * from {{source('TMDB_PROJECT','networks')}}

),

renamed as (
select
tv_show_id,
network_name,
load_date,
-- fixing network with 2 different id's
case when NETWORK_ID = 4330 then 6100 else NETWORK_ID end as NETWORK_ID,
WEEK_START_DATE as start_of_week_date
from source

)

select * from renamed

