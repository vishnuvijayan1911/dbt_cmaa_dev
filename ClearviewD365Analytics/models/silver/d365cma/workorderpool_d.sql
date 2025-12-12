{{ config(materialized='table', tags=['silver'], alias='workorderpool') }}

-- Source file: cma/cma/layers/_base/_silver/workorderpool/workorderpool.py
-- Root method: Workorderpool.workorderpooldetail [WorkOrderPoolDetail]
-- external_table_name: WorkOrderPoolDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['wop.recid']) }} AS WorkOrderPoolKey
         , wop.name        AS WorkOrderPool
         , wop.poolid      AS WorkOrderPoolID
         , wop.dataareaid AS LegalEntityID
         , wop.recid      AS _RecID
         , 1               AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('entassetworkorderpool') }} wop

