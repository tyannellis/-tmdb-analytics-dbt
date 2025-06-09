  with spoken_languages as (

    select 
     tv_show_id,
     language_name
    from {{ref("stg_tmdb_project__spoken_languages")}}
),

distinct_languages as (
select distinct
 tv_show_id,
 language_name
from spoken_languages )

select * from distinct_languages
