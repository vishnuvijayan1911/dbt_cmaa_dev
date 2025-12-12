{{ config(materialized='table', tags=['silver'], alias='workorder_fact') }}

-- Source file: cma/cma/layers/_base/_silver/workorder_f/workorder_f.py
-- Root method: WorkorderFact.workorder_factdetail [WorkOrder_FactDetail]
-- Inlined methods: WorkorderFact.workorder_factstage [WorkOrder_FactStage]
-- external_table_name: WorkOrder_FactDetail
-- schema_name: temp

WITH
workorder_factstage AS (
    SELECT WOT.dataareaid                 AS LegalEntityID
             , wota.workorderid                 AS ParentWorkOrderID
             , WOT.criticality
             , WOT.custaccount                  AS CustomerAccount
             , WOT.workorderlifecyclestate
             , aw.hcmworker                     AS RecID_HCMS
             , awr.hcmworker                    AS RecID_HCMR
             , WOT.workordertype
             , WOT.responsibleworkergroup
             , CAST(WOT.actualend AS DATE)      AS ActualEndDate
             , CAST(WOT.actualstart AS DATE)    AS ActualStartDate
             , CAST(WOT.expectedstart AS DATE)  AS ExpectedStartDate
             , CAST(WOT.expectedend AS DATE)    AS ExpectedEndDate
             , CAST(WOT.scheduledend AS DATE)   AS ScheduleEndDate
             , CAST(WOT.scheduledstart AS DATE) AS ScheduleStart
             , WOT.recid                    AS _RecID
             , 1                                AS _SourceID
             , CAST(CASE WHEN WOT.actualend = '1900-01-01 00:00:00.000'
                           OR WOT.actualend IS NULL
                         THEN CASE WHEN COALESCE(
                                            NULLIF(WOT.expectedend, '1900-01-01 00:00:00.000')
                                          , NULLIF(WOT.scheduledend, '1900-01-01 00:00:00.000')) < CAST(SYSDATETIME() AS DATE)
                                   THEN 2
                                   WHEN COALESCE(
                                            NULLIF(WOT.expectedend, '1900-01-01 00:00:00.000')
                                          , NULLIF(WOT.scheduledend, '1900-01-01 00:00:00.000')) >= CAST(SYSDATETIME() AS DATE)
                                   THEN 1
                                   ELSE 6 END
                         WHEN WOT.actualend <> '1900-01-01 00:00:00.000'
                         THEN CASE WHEN COALESCE(
                                            NULLIF(WOT.expectedend, '1900-01-01 00:00:00.000')
                                          , NULLIF(WOT.scheduledend, '1900-01-01 00:00:00.000')) < WOT.actualend
                                   THEN 3
                                   WHEN COALESCE(
                                            NULLIF(WOT.expectedend, '1900-01-01 00:00:00.000')
                                          , NULLIF(WOT.scheduledend, '1900-01-01 00:00:00.000')) >= WOT.actualend
                                   THEN 4
                                   ELSE 5 END
                         ELSE 0 END AS INT)     AS OnTimeStatusDueDate
             , CAST(CASE WHEN WOT.actualstart = '1900-01-01 00:00:00.000'
                           OR WOT.actualstart IS NULL
                         THEN CASE WHEN COALESCE(
                                            NULLIF(WOT.expectedstart, '1900-01-01 00:00:00.000')
                                          , NULLIF(WOT.scheduledstart, '1900-01-01 00:00:00.000')) < CAST(SYSDATETIME() AS DATE)
                                   THEN 8
                                   WHEN COALESCE(
                                            NULLIF(WOT.expectedstart, '1900-01-01 00:00:00.000')
                                          , NULLIF(WOT.scheduledstart, '1900-01-01 00:00:00.000')) >= CAST(SYSDATETIME() AS DATE)
                                   THEN 7
                                   ELSE 12 END
                         WHEN WOT.actualstart <> '1900-01-01 00:00:00.000'
                         THEN CASE WHEN COALESCE(
                                            NULLIF(WOT.expectedstart, '1900-01-01 00:00:00.000')
                                          , NULLIF(WOT.scheduledstart, '1900-01-01 00:00:00.000')) < WOT.actualstart
                                   THEN 9
                                   WHEN COALESCE(
                                            NULLIF(WOT.expectedstart, '1900-01-01 00:00:00.000')
                                          , NULLIF(WOT.scheduledstart, '1900-01-01 00:00:00.000')) >= WOT.actualstart
                                   THEN 10
                                   ELSE 11 END
                         ELSE 0 END AS INT)     AS OnTimeStatusStartDate

          FROM {{ ref('entassetworkordertable') }}      WOT
          LEFT JOIN {{ ref('entassetworkordertable') }} wota
            ON WOT.parentworkorder = wota.recid

          LEFT JOIN {{ ref('entassetworker') }}         aw
            ON aw.recid          = WOT.workerscheduled
          LEFT JOIN {{ ref('entassetworker') }}         awr
            ON awr.recid         = WOT.responsibleworker;
)
SELECT wo.WorkOrderKey
         , wo1.WorkOrderKey                                                     AS ParentWorkOrderKey
         , woc.WorkOrderCriticalityKey
         , dws.WorkOrderStateKey
         , dc.CustomerKey
         , le.LegalEntityKey
         , dwg.WorkerGroupKey
         , dwt.WorkOrderTypeKey
         , DATEDIFF(DAY, wo.CreateDate, NULLIF(wo.ActualEndDate, '1900-01-01')) AS WorkOrderProcessDays
         , res.EmployeeKey                                                      AS WorkerResponsibleKey
         , sch.EmployeeKey                                                      AS WORKERSCHEDULEDKey
         , ts._RecID
         , ts._SourceID
         , dd.DateKey                                                           AS ActualEndDateKey
         , dd1.DateKey                                                          AS ActualStartDateKey
         , dd2.DateKey                                                          AS ExpectedEndDateKey
         , dd3.DateKey                                                          AS ExpectedStartDateKey
         , dd4.DateKey                                                          AS ScheduleEndDateKey
         , dd5.DateKey                                                          AS ScheduleStartDateKey
         , CAST(ond.OnTimeStatusKey AS INT)                                     AS OnTimeStatusDueDateKey
         , CAST(ons.OnTimeStatusKey AS INT)                                     AS OnTimeStatusStartDateKey

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM workorder_factstage                      ts
     INNER JOIN {{ ref('legalentity_d') }}          le
        ON le.LegalEntityID   = ts.LegalEntityID
     INNER JOIN {{ ref('workorder_d') }}            wo
        ON wo._RecID          = ts._RecID
       AND wo._SourceID       = 1
      LEFT JOIN {{ ref('workordercriticality_d') }} woc
        ON woc._RecID         = ts.CRITICALITY
       AND woc._SourceID      = 1
      LEFT JOIN {{ ref('workorderstate_d') }}       dws
        ON dws._RecID         = ts.WORKORDERLIFECYCLESTATE
       AND dws._SourceID      = 1
      LEFT JOIN {{ ref('employee_d') }}             res
        ON res._RecID         = ts.RecID_HCMR
       AND res._SourceID      = 1
      LEFT JOIN {{ ref('employee_d') }}             sch
        ON sch._RecID         = ts.RecID_HCMS
       AND sch._SourceID      = 1
      LEFT JOIN {{ ref('customer_d') }}             dc
        ON dc.LegalEntityID   = ts.LegalEntityID
       AND dc.CustomerAccount = ts.CustomerAccount
      LEFT JOIN {{ ref('workergroup_d') }}          dwg
        ON dwg._RecID         = ts.RESPONSIBLEWORKERGROUP
       AND dwg._SourceID      = 1
      LEFT JOIN {{ ref('workordertype_d') }}        dwt
        ON dwt._RecID         = ts.WORKORDERTYPE
       AND dwt._SourceID      = 1
      LEFT JOIN {{ ref('date_d') }}                 dd
        ON dd.Date            = ts.ActualEndDate
      LEFT JOIN {{ ref('date_d') }}                 dd1
        ON dd1.Date           = ts.ActualStartDate
      LEFT JOIN {{ ref('date_d') }}                 dd2
        ON dd2.Date           = ts.ExpectedEndDate
      LEFT JOIN {{ ref('date_d') }}                 dd3
        ON dd3.Date           = ts.ExpectedStartDate
      LEFT JOIN {{ ref('date_d') }}                 dd4
        ON dd4.Date           = ts.ScheduleEndDate
      LEFT JOIN {{ ref('date_d') }}                 dd5
        ON dd5.Date           = ts.ScheduleStart
      LEFT JOIN {{ ref('workorder_d') }}            wo1
        ON wo1.LegalEntityID  = le.LegalEntityID
       AND wo1.WorkOrderID    = ts.ParentWorkOrderID
      LEFT JOIN {{ ref('on_time_status_d') }}         ond
        ON ond.OnTimeStatusID = ts.OnTimeStatusDueDate
      LEFT JOIN {{ ref('on_time_status_d') }}         ons
        ON ons.OnTimeStatusID = ts.OnTimeStatusStartDate;
