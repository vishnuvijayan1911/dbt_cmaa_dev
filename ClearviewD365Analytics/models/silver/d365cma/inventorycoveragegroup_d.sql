{{ config(materialized='table', tags=['silver'], alias='inventorycoveragegroup') }}

-- Source file: cma/cma/layers/_base/_silver/inventorycoveragegroup/inventorycoveragegroup.py
-- Root method: Inventorycoveragegroup.inventorycoveragegroupdetail [InventoryCoverageGroupDetail]
-- Inlined methods: Inventorycoveragegroup.inventorycoveragegroupstage [InventoryCoverageGroupStage]
-- external_table_name: InventoryCoverageGroupDetail
-- schema_name: temp

WITH
inventorycoveragegroupstage AS (
    SELECT rg.dataareaid  AS LegalEntityID
             , rg.reqgroupid  AS InventoryCoverageGroupID
             , rg.name        AS InventoryCoverageGroup
             , rg.covrule     AS InventoryCoverageRuleID
             , rg.recid       AS _RecID
             , 1              AS _SourceID

          FROM {{ ref('reqgroup') }} rg
)
SELECT ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS InventoryCoverageGroupKey
         , le.LegalEntityID            AS LegalEntityID
         , ts.InventoryCoverageGroupID AS InventoryCoverageGroupID
         , ts.InventoryCoverageGroup   AS InventoryCoverageGroup
         , we.enumvalueid              AS InventoryCoverageRuleID
         , we.enumvalue                AS InventoryCoverageRule
         , ts._SourceID                AS _SourceID
         , ts._RecID                   AS _RecID

        ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM inventorycoveragegroupstage               ts
     INNER JOIN {{ ref('legalentity_d') }} le
        ON le.LegalEntityID = ts.LegalEntityID
      LEFT JOIN {{ ref('enumeration') }} we
        ON we.enumvalueid   = ts.InventoryCoverageRuleID
       AND we.enum          = 'ReqCovType';

