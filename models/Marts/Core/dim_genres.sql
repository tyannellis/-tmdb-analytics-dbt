with genres as (

    select * from {{ref("stg_tmdb_project__genres")}}
),

distinct_genres as (

select distinct 
genre_id,
genre_name
from genres

)

select * from distinct_genres