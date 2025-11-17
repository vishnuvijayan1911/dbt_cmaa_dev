{{ config(materialized='table', tags=['silver'], alias='productionschedulestatus') }}

SELECT *
  FROM silver.cma_ProductionScheduleStatus;
