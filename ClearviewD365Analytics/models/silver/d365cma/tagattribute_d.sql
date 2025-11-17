{{ config(materialized='table', tags=['silver'], alias='tagattribute') }}

-- TODO: replace with real logic for tagattribute_d
select *
from silver.tagattribute;
