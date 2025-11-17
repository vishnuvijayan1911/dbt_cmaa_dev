{{ config(materialized='table', tags=['silver'], alias='productiontype') }}

SELECT *
  FROM silver.cma_ProductionType;
