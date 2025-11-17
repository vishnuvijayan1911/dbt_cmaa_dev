{{ config(materialized='table', tags=['silver'], alias='vwuomconversion_ft') }}

SELECT *
  FROM silver.cma_vwUOMConversion_FT;

