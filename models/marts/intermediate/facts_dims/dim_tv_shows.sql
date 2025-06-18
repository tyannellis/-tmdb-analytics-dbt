{{ config(
    materialized = 'incremental',
    unique_key = 'surrogate_key',
    incremental_strategy = 'merge'
) }}

with tv_show_source as (

select
        tv_show_id,
        case when remade_show_rank > 1 then original_name || ' (' || year(FIRST_EPISODE_AIR_DATE) || ')'  else original_name end as original_name,
        case when remade_show_rank > 1 then localized_name || ' (' || year(FIRST_EPISODE_AIR_DATE) || ')' else localized_name end as localized_name,
        number_of_episodes,
        number_of_seasons,
        FIRST_EPISODE_AIR_DATE,
        ORIGINAL_LANGUAGE_CODE,
        language_name as ORIGINAL_LANGUAGE_name,
        tv_show_status,
        tv_show_type,
        current_date as record_start_date,
        null as record_end_date,
        true as is_current,
        {{ dbt_utils.generate_surrogate_key([
            'tv_show_id',
            'number_of_episodes',
            'number_of_seasons',
            'tv_show_status'
        ]) }} as surrogate_key

from (select *,
row_number() over (partition by tv_show_id order by start_of_week_date desc) as row_num,
row_number() over (partition by original_name order by FIRST_EPISODE_AIR_DATE) as remade_show_rank
from {{ref("stg_tmdb_project__trending_tv_shows")}}) as tv_base
left join {{ref('language_code')}}
using (ORIGINAL_LANGUAGE_CODE)
where row_num = 1

),

{% if is_incremental() %}
existing_dim as (
    select *
    from {{ this }}
    where is_current = true
),
{% endif %}

{% if is_incremental() %}
changed_records as (
    select s.*
    from tv_show_source s
    join existing_dim e
      on s.tv_show_id = e.tv_show_id
    where 
        s.number_of_episodes != e.number_of_episodes
        or s.number_of_seasons != e.number_of_seasons
        or s.tv_show_status != e.tv_show_status
),
 {% endif %}


{% if is_incremental() %}
expired_records as (
    select 
        tv_show_id,
        original_name,
        localized_name,
        number_of_episodes,
        number_of_seasons,
        FIRST_EPISODE_AIR_DATE,
        ORIGINAL_LANGUAGE_CODE,
        ORIGINAL_LANGUAGE_name,
        tv_show_status,
        tv_show_type,
        record_start_date, 
        current_date as record_end_date,
        false as is_current,
        surrogate_key
    from existing_dim
    where tv_show_id in (select tv_show_id from changed_records)
),
{% endif %}


{% if is_incremental() %}
new_and_updated_records as (
    select * from changed_records
    union all 
    select * from tv_show_source
    where tv_show_id not in (select tv_show_id from existing_dim)
)
{% else %}
new_and_updated_records as (
    select * from tv_show_source
)
{% endif %}

select * from new_and_updated_records
{% if is_incremental() %}
union all 
select * from expired_records
{% endif %}

