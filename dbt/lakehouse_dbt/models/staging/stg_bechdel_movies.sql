with source as (
    select *
    from read_parquet('{{ var("silver_bechdel_movies_path") }}')
)

select
    imdb,
    title,
    cast(year as integer) as release_year,
    cast(strptime(cast(year as varchar) || '-01-01', '%Y-%m-%d') as date) as release_date,
    clean_test,
    "binary" as bechdel_result,
    cast(budget_2013 as double) as budget_2013,
    cast(domgross_2013 as double) as domgross_2013,
    cast(intgross_2013 as double) as intgross_2013,
    _ingested_at,
    _batch_id
from source
