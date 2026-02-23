with source as (
    select *
    from read_parquet('{{ repo_root() }}/lakehouse/silver/recent_grads/data.parquet')
)

select
    cast(major_code as integer) as major_code,
    major,
    major_category,
    cast(total as bigint) as total_graduates,
    cast(employed as bigint) as employed_graduates,
    cast(unemployed as bigint) as unemployed_graduates,
    cast(unemployment_rate as double) as unemployment_rate,
    cast(median as double) as median_salary,
    cast(sharewomen as double) as share_women,
    cast(low_wage_jobs as bigint) as low_wage_jobs,
    cast(college_jobs as bigint) as college_jobs,
    cast(non_college_jobs as bigint) as non_college_jobs,
    _ingested_at,
    _batch_id
from source
