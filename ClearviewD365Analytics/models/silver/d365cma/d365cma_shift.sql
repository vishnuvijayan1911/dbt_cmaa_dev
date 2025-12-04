{{ config(materialized='table', tags=['silver'], alias='shift_source') }}

SELECT *
  FROM silver.cma_Shift;
