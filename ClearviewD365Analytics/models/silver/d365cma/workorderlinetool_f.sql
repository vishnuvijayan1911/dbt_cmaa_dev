{{ config(materialized='table', tags=['silver'], alias='workorderlinetool_fact') }}

-- Source file: cma/cma/layers/_base/_silver/workorderlinetool_f/workorderlinetool_f.py
-- Root method: WorkorderlinetoolFact.workorderlinetool_factdetail [WorkOrderLineTool_FactDetail]
-- external_table_name: WorkOrderLineTool_FactDetail
-- schema_name: temp

SELECT dwo.WorkOrderLineKey      AS WorkOrderLineKey
         , wop.ProductionResourceKey AS ResourceKey
         , wt.recid                  AS _RecID
         , 1                         AS _SourceID
         ,CURRENT_TIMESTAMP           AS _CreatedDate
         ,CURRENT_TIMESTAMP           AS _ModifiedDate

      FROM {{ ref('entassetworkorderlinetool') }} wt
     INNER JOIN silver.cma_WorkOrderLine_Fact   dwo
        ON dwo._RecID        = wt.workorderline
       AND dwo._SourceID     = 1
     INNER JOIN silver.cma_ProductionResource   wop
        ON wop.LegalEntityID = wt.dataareaid
       AND wop.ResourceID    = wt.wrkctrid;
