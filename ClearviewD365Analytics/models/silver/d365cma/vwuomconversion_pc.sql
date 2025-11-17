{{ config(materialized='table', tags=['silver'], alias='vwuomconversion_pc') }}

SELECT *
  FROM silver.cma_vwUOMConversion_PC;

