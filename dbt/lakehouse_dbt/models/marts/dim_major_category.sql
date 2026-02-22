select
    major_category,
    count(*) as majors_count
from {{ ref('stg_recent_grads') }}
group by major_category
order by major_category
