{{ config(materialized='table', tags=['silver'], alias='productattribute') }}

-- TODO: replace with real logic for productattribute_d
select *
from silver.productattribute;
