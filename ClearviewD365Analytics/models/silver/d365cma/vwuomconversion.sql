{{ config(materialized='table', tags=['silver'], alias='vwuomconversion') }}

SELECT *
  FROM silver.cma_vwUOMConversion;

