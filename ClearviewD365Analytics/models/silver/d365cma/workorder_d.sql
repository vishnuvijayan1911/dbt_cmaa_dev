{{ config(materialized='table', tags=['silver'], alias='workorder') }}

-- Source file: cma/cma/layers/_base/_silver/workorder/workorder.py
-- Root method: Workorder.workorderdetail [WorkOrderDetail]
-- Inlined methods: Workorder.workorderslogfirstrecord [WorkOrderSLOGFirstRecord], Workorder.workorderslog [WorkOrderslog]
-- external_table_name: WorkOrderDetail
-- schema_name: temp

WITH
workorderslogfirstrecord AS (
    SELECT MIN (created) AS CreatedDate
             , refrecid
             , dataareaid

          FROM {{ ref('entassetlifecyclestatelog') }}
         GROUP BY refrecid
                , dataareaid
),
workorderslog AS (
    SELECT MAX (created)         AS CreatedDate
             , refrecid
             , reftableid
             , lifecyclestaterefrecid
             , createdby             AS CreatedBy
             , remark

          FROM {{ ref('entassetlifecyclestatelog') }}
         WHERE (remark LIKE 'Work Order Created%' OR remark LIKE 'Arbejdsordre oprettet%')
         GROUP BY refrecid
                , reftableid
                , lifecyclestaterefrecid
                , createdby
                , remark
)
<<<<<<< HEAD
SELECT ROW_NUMBER() OVER (ORDER BY WOT.recid) AS WorkOrderKey
         , WOT.dataareaid                                                                                                      AS LegalEntityID
=======
SELECT {{ dbt_utils.generate_surrogate_key(['WOT.recid']) }} AS WorkOrderKey
         , WOT.dataareaid                                                                                                     AS LegalEntityID
>>>>>>> db49a969de400983631fe6fe6226a8c58cf95fc1
         , WOT.workorderid                                                                                                     AS WorkOrderID
         , CASE WHEN WOT.active = 'Yes' THEN 'Active' ELSE 'Inactive' END                                                      AS ActiveStatus
         , WOT.costtype                                                                                                        AS CostType
         , WOT.description                                                                                                     AS WorkOrderDesc
         , WOTA.workorderid                                                                                                    AS ParentWorkorderID
         , WOT.scheduleoneworker                                                                                               AS ScheduleOneWorker
         , CASE WHEN WOLS.workorderlifecyclestateid IN ( 'Closed', 'Ended', 'Finished', 'Cancelled' ) THEN 'Closed' ELSE
                                                                                                                    'Open' END AS ClosedStatus
         , CAST(WOT.actualend AS DATE)                                                                                         AS ActualEndDate
         , CAST(WOT.actualstart AS DATE)                                                                                       AS ActualStartDate
         , CAST(WOT.expectedend AS DATE)                                                                                       AS ExpectEndDate
         , CAST(WOT.expectedstart AS DATE)                                                                                     AS ExpectStartDate
         , CAST(WOT.scheduledend AS DATE)                                                                                      AS ScheduleEndDate
         , CAST(WOT.scheduledstart AS DATE)                                                                                    AS ScheduleStartDate
         , wos.name                                                                                                            AS WorkOrderServiceLevel
         , wos.servicelevel                                                                                                    AS WorkOrderServiceLevelID
         , SLOG.CreatedBy                                                                                                      AS CreateBy
         , CAST(ISNULL (SLOG.CreatedDate, SLOGFirstRecord.CreatedDate) AS DATE)                                                AS CreateDate
         , WOT.recid                                                                                                           AS _RecID
         , 1                                                                                                                   AS _SourceID
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                                                                                                               AS  _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                                                                                                               AS  _ModifiedDate
      FROM {{ ref('entassetworkordertable') }}               WOT


      LEFT JOIN {{ ref('entassetworkorderlifecyclestate') }} WOLS
        ON WOT.workorderlifecyclestate = WOLS.recid
      LEFT JOIN {{ ref('entassetworkordertable') }}          WOTA
        ON WOT.parentworkorder         = WOTA.recid
      LEFT JOIN workorderslog                               SLOG
        ON WOT.recid                  = SLOG.refrecid
      LEFT JOIN workorderslogfirstrecord                    SLOGFirstRecord
        ON WOT.recid                  = SLOGFirstRecord.refrecid
       AND WOT.dataareaid             = SLOGFirstRecord.dataareaid
      LEFT JOIN {{ ref('entassetworkorderservicelevel') }}   wos
        ON wos.dataareaid             = WOT.dataareaid
       AND wos.servicelevel            = WOT.servicelevel

