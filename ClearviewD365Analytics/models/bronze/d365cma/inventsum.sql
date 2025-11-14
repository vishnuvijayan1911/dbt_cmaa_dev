{{ config(materialized='view') }}

select  *
from {{ source("lakehouse","inventsum") }};
