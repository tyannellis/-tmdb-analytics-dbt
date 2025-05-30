WITH source as (
    select  * from {{ source('TMDB_PROJECT', 'genres') }}
),


renamed as (
Select
tv_show_id,
genre_id,
genre_name,
load_date,
WEEK_START_DATE as start_of_week_date
from source 
)

select * from renamed 





	