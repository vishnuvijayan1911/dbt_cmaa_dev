{{ config(materialized='view') }}

select  *
from {{ source("lakehouse","acojournalname_br") }};
