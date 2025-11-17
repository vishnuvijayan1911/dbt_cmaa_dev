{{ config(materialized='table', tags=['silver'], alias='vwuomconversion_sqin') }}

SELECT *
  FROM silver.cma_vwUOMConversion_SQIN;

