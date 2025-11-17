{{ config(materialized='table', tags=['silver'], alias='productionremainingstatus') }}

SELECT *
  FROM silver.cma_ProductionRemainingStatus;
