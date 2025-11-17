{{ config(materialized='table', tags=['silver'], alias='workorderstate') }}

-- Source file: cma/cma/layers/_base/_silver/workorderstate/workorderstate.py
-- Root method: Workorderstate.workorderstatedetail [WorkOrderStateDetail]
-- external_table_name: WorkOrderStateDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY JT.recid) AS WorkOrderStateKey
          ,JT.dataareaid                                                                                        AS LegalEntityID
         , REPLACE(REPLACE(JT.workorderlifecyclestateid, 'InProgress', 'In-progress'), 'Cancelled', 'Canceled') AS WorkOrderStateID
         , ISNULL(NULLIF(JT.name, ''), JT.workorderlifecyclestateid)                                            AS WorkOrderState
         , JT.recid                                                                                             AS _RecID
         , 1                                                                                                    AS _SourceID
         ,  CURRENT_TIMESTAMP                                                                                      AS  _CreatedDate
         ,  CURRENT_TIMESTAMP                                                                                      AS  _ModifiedDate

      FROM {{ ref('entassetworkorderlifecyclestate') }} JT

