{{ config(materialized='table', tags=['silver'], alias='vwuomconversion_lb') }}

SELECT *
  FROM silver.cma_vwUOMConversion_LB;

