{{ config(materialized='table', tags=['silver'], alias='inventoryreferencetype') }}

SELECT *
  FROM silver.cma_InventoryReferenceType;
