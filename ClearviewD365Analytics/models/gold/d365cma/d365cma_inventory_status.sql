{{ config(materialized='view', schema='gold', alias="Inventory status") }}

SELECT  t.InventoryStatusKey            AS [Inventory status key]
  , NULLIF(t.LegalEntityID, '')     AS [Legal entity]
  , NULLIF(t.InventoryStatusID, '') AS [Inventory status]
  , NULLIF(t.InventoryStatus, '')   AS [Inventory status name]
FROM {{ ref("d365cma_inventorystatus_d") }} t;
