with latest_trending_show_week as (
select *
from {{ref("fct_trending_tv_shows")}}
qualify row_number()over(partition by tv_show_id order by trending_week_start_date desc) = 1


),

creator_base as (
select 
creators_bridge.tv_show_id,
creator_dim.creator_id,
creator_dim.creator_name,
from {{ ref('int_bridge_show_creators') }} as creators_bridge
join {{ref('dim_creators')}} creator_dim
using (creator_id)

)


select 
creator_id,
creator_name,
avg(lifetime_vote_average) creator_voting_average,
sum(lifetime_vote_count) creator_vote_count,
ROUND(
  COALESCE(SUM(CASE WHEN lt.lifetime_vote_average >= 8 THEN 1 ELSE 0 END)::float
  / COUNT(DISTINCT cb.tv_show_id), 0.0) * 100,1) AS pct_high_rated_shows,
count(distinct cb.tv_show_id ) as tv_show_count
from creator_base as cb
left join latest_trending_show_week as lt 
using (tv_show_id)
group by 
creator_id,
creator_name


