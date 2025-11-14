{{ config(materialized='view') }}

select  *
from {{ source("lakehouse","oracle_enumeration") }};
