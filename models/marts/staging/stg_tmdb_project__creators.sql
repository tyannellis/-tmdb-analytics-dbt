with source as (
    select * from {{source ('TMDB_PROJECT','creators')}}
),

renamed as (
select 
tv_show_id,
creator_id,
creator_name,
load_date,
WEEK_START_DATE as start_of_week_date
from source
)

select * from renamed