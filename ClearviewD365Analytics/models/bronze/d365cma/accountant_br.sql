{{ config(materialized='view') }}

select  *
from {{ source("lakehouse","accountant_br") }};
