{{ config(materialized='table', tags=['silver'], alias='workorderpool') }}

-- Source file: cma/cma/layers/_base/_silver/workorderpool/workorderpool.py
-- Root method: Workorderpool.workorderpooldetail [WorkOrderPoolDetail]
-- external_table_name: WorkOrderPoolDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY wop.recid) AS WorkOrderPoolKey
         , wop.name        AS WorkOrderPool
         , wop.poolid      AS WorkOrderPoolID
         , wop.dataareaid AS LegalEntityID
         , wop.recid      AS _RecID
         , 1               AS _SourceID

      FROM {{ ref('entassetworkorderpool') }} wop

