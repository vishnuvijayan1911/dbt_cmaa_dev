{{ config(materialized='view') }}

select  *
from {{ source("lakehouse","acojournaltable_br") }};
