{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/inventorystatus/inventorystatus.py
-- Root method: Inventorystatus.inventorystatusdetail [InventoryStatusDetail]
-- external_table_name: InventoryStatusDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY ws.recid) AS InventoryStatusKey
         , ws.dataareaid                                                  AS LegalEntityID
         , CASE WHEN ws.name = '' THEN ws.inventstatusid ELSE ws.name END AS InventoryStatus
         , ws.inventstatusid                                              AS InventoryStatusID
         , ws.recid                                                       AS _RecID
         , 1                                                              AS _SourceID
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM {{ ref('whsinventstatus') }} ws
