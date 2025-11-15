{{ config(materialized='table', tags=['silver'], alias='workorder_dim') }}

-- Source file: cma/cma/layers/_base/_silver/workorder/workorder.py
-- Root method: Workorder.workorderdetail [WorkOrderDetail]
-- Inlined methods: Workorder.workorderslogfirstrecord [WorkOrderSLOGFirstRecord], Workorder.workorderslog [WorkOrderslog]
-- external_table_name: WorkOrderDetail
-- schema_name: temp

WITH
workorderslogfirstrecord AS (
    SELECT MIN (CREATED) AS CreatedDate
             , REFRECID
             , DATAAREAID

          FROM {{ ref('entassetlifecyclestatelog') }}
         GROUP BY REFRECID
                , DATAAREAID;
),
workorderslog AS (
    SELECT MAX (CREATED)         AS CreatedDate
             , REFRECID
             , REFTABLEID
             , LIFECYCLESTATEREFRECID
             , CREATEDBY             AS CreatedBy
             , REMARK

          FROM {{ ref('entassetlifecyclestatelog') }}
         WHERE (REMARK LIKE 'Work Order Created%' OR REMARK LIKE 'Arbejdsordre oprettet%')
         GROUP BY REFRECID
                , REFTABLEID
                , LIFECYCLESTATEREFRECID
                , CREATEDBY
                , REMARK;
)
SELECT ROW_NUMBER() OVER (ORDER BY WOT.recid) AS WorkOrderKey
         , WOT.dataareaid                                                                                                     AS LegalEntityID
         , WOT.workorderid                                                                                                     AS WorkOrderID
         , CASE WHEN WOT.active = 1 THEN 'Active' ELSE 'Inactive' END                                                          AS ActiveStatus
         , enum.enumvalue                                                                                                      AS CostType
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
         ,  CURRENT_TIMESTAMP                                                                                                                                                               AS  _CreatedDate
         ,  CURRENT_TIMESTAMP                                                                                                                                                               AS  _ModifiedDate

      FROM {{ ref('entassetworkordertable') }}               WOT


      LEFT JOIN {{ ref('entassetworkorderlifecyclestate') }} WOLS
        ON WOT.workorderlifecyclestate = WOLS.recid
      LEFT JOIN {{ ref('entassetworkordertable') }}          WOTA
        ON WOT.parentworkorder         = WOTA.recid
      LEFT JOIN workorderslog                               SLOG
        ON WOT.recid                  = SLOG.REFRECID
      LEFT JOIN workorderslogfirstrecord                    SLOGFirstRecord
        ON WOT.recid                  = SLOGFirstRecord.REFRECID
       AND WOT.dataareaid             = SLOGFirstRecord.DATAAREAID
      LEFT JOIN {{ ref('enumeration') }}                     enum
        ON enum.enum                   = 'AssetCostType'
       AND enum.enumvalueid            = WOT.costtype
      LEFT JOIN {{ ref('entassetworkorderservicelevel') }}   wos
        ON wos.dataareaid             = WOT.dataareaid
       AND wos.servicelevel            = WOT.servicelevel;
