{{ config(materialized='table', tags=['silver'], alias='workorderlinetool_fact') }}

-- Source file: cma/cma/layers/_base/_silver/workorderlinetool_f/workorderlinetool_f.py
-- Root method: WorkorderlinetoolFact.workorderlinetool_factdetail [WorkOrderLineTool_FactDetail]
-- external_table_name: WorkOrderLineTool_FactDetail
-- schema_name: temp

SELECT dwo.WorkOrderLineKey      AS WorkOrderLineKey
         , wop.ProductionResourceKey AS ResourceKey
         , wt.recid                  AS _RecID
         , 1                         AS _SourceID

         ,cast(CURRENT_TIMESTAMP as DATETIME2(6))           AS _CreatedDate
         ,cast(CURRENT_TIMESTAMP as DATETIME2(6))           AS _ModifiedDate
      FROM {{ ref('entassetworkorderlinetool') }} wt
     INNER JOIN {{ ref('d365cma_workorderline_f') }}   dwo
        ON dwo._RecID        = wt.workorderline
       AND dwo._SourceID     = 1
     INNER JOIN {{ ref('d365cma_productionresource_d') }}   wop
        ON wop.LegalEntityID = wt.dataareaid
       AND wop.ResourceID    = wt.wrkctrid;
