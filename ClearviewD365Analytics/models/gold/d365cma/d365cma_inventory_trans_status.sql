{{ config(materialized='view', schema='gold', alias="Inventory trans status") }}

SELECT  t.InventorytransStatusKey               AS [Inventory trans status key]
    , NULLIF(t.InventoryTransStatusID, '')    AS [Inventory trans status ID]
    , NULLIF(t.InventoryTransStatus, '')      AS [Inventory trans status]
    , NULLIF(t.InventoryTransStatusDesc, '')  AS [Inventory trans status desc]
    , NULLIF(t.InventoryTransStatusGroup, '') AS [Inventory trans status group]
    , NULLIF(t.InventoryTransStatusName, '')  AS [Inventory trans status name]
    , NULLIF(t.InventoryTransStatusType, '')  AS [Inventory trans status type]
  FROM {{ ref("d365cma_inventory_trans_status_d") }} t;
