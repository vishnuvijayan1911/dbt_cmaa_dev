{{ config(materialized='table', tags=['silver'], alias='workorderpool_fact') }}

-- Source file: cma/cma/layers/_base/_silver/workorderpool_f/workorderpool_f.py
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

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM workorderpool_factstage                ts
     INNER JOIN {{ ref('legalentity_d') }}   le
        ON le.LegalEntityID = ts.DATAAREAID
      LEFT JOIN {{ ref('workorder_d') }}     dwo
        ON dwo._RecID       = ts.WORKORDER
       AND dwo._SourceID    = 1
      LEFT JOIN {{ ref('workorderpool_d') }} wop
        ON wop._RecID       = ts.WorkOrderPool
       AND wop._SourceID    = 1;
