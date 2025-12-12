{{ config(materialized='table', tags=['silver'], alias='workorderstate') }}

-- Source file: cma/cma/layers/_base/_silver/workorderstate/workorderstate.py
-- Root method: Workorderstate.workorderstatedetail [WorkOrderStateDetail]
-- external_table_name: WorkOrderStateDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['JT.recid']) }} AS WorkOrderStateKey
          ,JT.dataareaid                                                                                        AS LegalEntityID
         , REPLACE(REPLACE(JT.workorderlifecyclestateid, 'InProgress', 'In-progress'), 'Cancelled', 'Canceled') AS WorkOrderStateID
         , ISNULL(NULLIF(JT.name, ''), JT.workorderlifecyclestateid)                                            AS WorkOrderState
         , JT.recid                                                                                             AS _RecID
         , 1                                                                                                    AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('entassetworkorderlifecyclestate') }} JT

