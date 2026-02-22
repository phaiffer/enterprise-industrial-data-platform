select
    major_code,
    major,
    major_category,
    total_graduates,
    employed_graduates,
    unemployed_graduates,
    unemployment_rate,
    median_salary,
    share_women,
    low_wage_jobs,
    college_jobs,
    non_college_jobs
from {{ ref('stg_recent_grads') }}
