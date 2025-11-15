{{ config(materialized='table', tags=['silver'], alias='inventorycountinggroup_dim') }}

-- Source file: cma/cma/layers/_base/_silver/inventorycountinggroup/inventorycountinggroup.py
-- Root method: Inventorycountinggroup.inventorycountinggroupdetail [InventoryCountingGroupDetail]
-- Inlined methods: Inventorycountinggroup.inventorycountinggroupstage [InventoryCountingGroupStage]
-- external_table_name: InventoryCountingGroupDetail
-- schema_name: temp

WITH
inventorycountinggroupstage AS (
    SELECT 
               icg.dataareaid  AS LegalEntityID
             , icg.countgroupid AS InventoryCountingGroupID
             , icg.name         AS InventoryCountingGroup
             , icg.recid       AS _RecID
             , 1                AS _SourceID
          FROM {{ ref('inventcountgroup') }} icg
)
SELECT ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS InventoryCountingGroupKey
         , ts.LegalEntityID            AS LegalEntityID
         , ts.InventoryCountingGroupID AS InventoryCountingGroupID
         , ts.InventoryCountingGroup   AS InventoryCountingGroup
         , ts._SourceID                AS _SourceID
         , ts._RecID                   AS _RecID
        ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate         
      FROM inventorycountinggroupstage              ts
     INNER JOIN silver.cma_LegalEntity le
        ON le.LegalEntityID = ts.LegalEntityID;
