SELECT *
FROM {{ ref('fct_trending_tv_shows') }}
WHERE lifetime_vote_average < 1 OR lifetime_vote_average > 10