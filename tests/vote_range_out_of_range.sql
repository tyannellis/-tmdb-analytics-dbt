SELECT *
FROM {{ ref('fct_trending_tv_shows') }}
WHERE vote_average < 1 OR vote_average > 10