select
    count(*) as total_majors,
    round(avg(median_salary), 2) as avg_median_salary,
    round(avg(unemployment_rate), 4) as avg_unemployment_rate,
    round(sum(low_wage_jobs) * 1.0 / nullif(sum(employed_graduates), 0), 4) as low_wage_job_share,
    round(avg(share_women), 4) as avg_share_women
from {{ ref('fact_major_employment') }}
