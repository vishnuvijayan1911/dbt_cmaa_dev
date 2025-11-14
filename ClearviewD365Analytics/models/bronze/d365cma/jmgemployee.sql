{{ config(materialized='view') }}

select  *
from {{ source("lakehouse","jmgemployee") }};
