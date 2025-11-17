{{ config(materialized='view', schema='gold', alias="Inventory site") }}

SELECT t.InventorySiteKey                          AS [Inventory site key]
     , CONCAT (t.LegalEntityID, t.InventorySiteID) AS [Inventory site index]
     , NULLIF (t.LegalEntityID, '')                AS [Legal entity]
     , NULLIF (t.InventorySiteID, '')              AS [Inventory site]
     , NULLIF (t.InventorySite, '')                AS [Inventory site name]
  FROM {{ ref("inventorysite_d") }} t;
