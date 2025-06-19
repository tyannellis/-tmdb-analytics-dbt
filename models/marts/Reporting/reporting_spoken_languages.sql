
with most_recent_trending AS (
select *
from {{ref("fct_trending_tv_shows")}}
qualify row_number()over(partition by tv_show_id order by trending_week_start_date desc) = 1

)


select
Language_name,
count(distinct tv_show_id) as number_of_tv_shows,
sum(lifetime_vote_count) as total_votes_per_language,
round(avg(lifetime_vote_average),2) as average_vote_per_language,
ROUND(
  COALESCE(SUM(CASE WHEN lifetime_vote_average >= 8 THEN 1 ELSE 0 END)::float
  / COUNT(DISTINCT tv_show_id), 0.0) * 100,1) AS pct_high_rated_shows,
 from {{ref("int_bridge_show_spoken_languages")}}
 left join most_recent_trending 
 using (tv_show_id)
 group by
Language_name