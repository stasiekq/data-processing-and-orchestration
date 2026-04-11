{{ config(materialized='view') }}

select *
from read_parquet('{{ var("silver_glob") }}', hive_partitioning=true)
