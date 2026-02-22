select
    count(*) as total_movies,
    sum(case when bechdel_result = 'PASS' then 1 else 0 end) as pass_movies,
    round(sum(case when bechdel_result = 'PASS' then 1 else 0 end) * 1.0 / nullif(count(*), 0), 4) as pass_rate,
    round(avg(domgross_2013), 2) as avg_domgross_2013,
    max(release_date) as latest_release_date
from {{ ref('stg_bechdel_movies') }}
