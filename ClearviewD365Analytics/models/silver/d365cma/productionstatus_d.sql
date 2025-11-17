{{ config(materialized='table', tags=['silver'], alias='productionstatus') }}

SELECT *
  FROM silver.cma_ProductionStatus;
