{{ config(materialized='view', schema='gold', alias="Work order") }}

SELECT  t.WorkOrderKey                                                                                      AS [Work order key]
    , NULLIF(t.WorkOrderID, '')                                                                           AS [Work order #]
    , NULLIF(t.ActiveStatus, '')                                                                          AS [Work order active status]
    , NULLIF(t.ParentWorkOrderID, '')                                                                     AS [Parent work order #]
    , NULLIF(t.ScheduleOneWorker, '')                                                                     AS [Schedule one worker]
    , NULLIF(t.ClosedStatus, '')                                                                          AS [Work order closed status]
    , NULLIF(t.WorkOrderDesc, '')                                                                         AS [Work order desc]
    , NULLIF(wg.WorkerGroupID, '')                                                                        AS [Worker group]
    , NULLIF(wos.WorkOrderStateID, '')                                                                    AS [Work order state]
    , NULLIF(c.WorkOrderCriticality, '')                                                                  AS [Work order criticality]
    , NULLIF(t.WorkOrderServiceLevelID, 0)                                                                AS [Work order service level]
    , NULLIF(t.ActualEndDate, '1/1/1900')                                                                 AS [Actual end date]
    , NULLIF(t.ActualStartDate, '1/1/1900')                                                               AS [Actual start date]
    , NULLIF(t.CreateDate, '1/1/1900')                                                                    AS [Create date]
    , NULLIF(t.CreateBy, '')                                                                              AS [Create by]
    , NULLIF(t.ExpectEndDate, '1/1/1900')                                                                 AS [Expect end date]
    , NULLIF(t.ExpectStartDate, '1/1/1900')                                                               AS [Expect start date]
    , NULLIF(t.ScheduleEndDate, '1/1/1900')                                                               AS [Schedule end date]
    , NULLIF(t.ScheduleStartDate, '1/1/1900')                                                             AS [Schedule start date]
    , NULLIF(REPLACE (REPLACE (wot.WorkOrderType, ' work orders', ''), ' Corrective', ' corrective'), '') AS [Work order type]
  FROM {{ ref("workorder_d") }}                 t 
INNER JOIN {{ ref("workorder_f") }}       f 
    ON f.WorkOrderKey            = t.WorkOrderKey
  LEFT JOIN {{ ref("workergroup_d") }}          wg
    ON wg.WorkerGroupKey         = f.WorkerGroupKey
  LEFT JOIN {{ ref("workordercriticality_d") }} c 
    ON c.WorkOrderCriticalityKey = f.WorkOrderCriticalityKey
  LEFT JOIN {{ ref("workordertype_d") }}        wot 
    ON wot.WorkOrderTypeKey      = f.WorkOrderTypeKey
  LEFT JOIN {{ ref("workorderstate_d") }}       wos 
    ON wos.WorkOrderStateKey     = f.WorkOrderStateKey;
