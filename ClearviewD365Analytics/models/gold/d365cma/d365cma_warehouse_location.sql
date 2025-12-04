{{ config(materialized='view', schema='gold', alias="Warehouse location") }}

SELECT  t.WarehouseLocationKey          AS [Warehouse location key]
  , NULLIF(t.WarehouseLocation, '') AS [Location]
  , NULLIF(t.Site, '')              AS [Site]
FROM {{ ref("d365cma_warehouselocation_d") }} t;
