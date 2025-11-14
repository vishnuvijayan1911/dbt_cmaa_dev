{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/workorderpool_fact/workorderpool_fact.py
-- Root method: WorkorderpoolFact.workorderpool_factdetail [WorkOrderPool_FactDetail]
-- Inlined methods: WorkorderpoolFact.workorderpool_factstage [WorkOrderPool_FactStage]
-- external_table_name: WorkOrderPool_FactDetail
-- schema_name: temp

WITH
workorderpool_factstage AS (
    SELECT wopr.workorderpool
             , wopr.workorder
             , wopr.dataareaid 
             , wopr.recid AS _RecID
             , 1           AS _SourceID

          FROM {{ ref('entassetworkorderpoolrelation') }} wopr;
)
SELECT le.LegalEntityKey
         , dwo.WorkOrderKey
         , wop.WorkOrderPoolKey
         , ts._RecID
         , ts._SourceID
         , CURRENT_TIMESTAMP   AS _CreatedDate
         , CURRENT_TIMESTAMP   AS _ModifiedDate

      FROM workorderpool_factstage                ts
     INNER JOIN silver.cma_LegalEntity   le
        ON le.LegalEntityID = ts.DATAAREAID
      LEFT JOIN silver.cma_WorkOrder     dwo
        ON dwo._RecID       = ts.WORKORDER
       AND dwo._SourceID    = 1
      LEFT JOIN silver.cma_WorkOrderPool wop
        ON wop._RecID       = ts.WorkOrderPool
       AND wop._SourceID    = 1;
