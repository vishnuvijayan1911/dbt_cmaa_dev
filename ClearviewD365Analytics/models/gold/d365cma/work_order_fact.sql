{{ config(materialized='view', schema='gold', alias="Work order fact") }}

SELECT  wof.WorkOrderKey                                                                                             AS [Work order key]
    , wof.LegalEntityKey                                                                                           AS [Legal entity key]
    , wof.ActualEndDateKey                                                                                         AS [Work order actual end date key]
    , wof.ActualStartDateKey                                                                                       AS [Work order actual start date key]
    , wof.ExpectedEndDateKey                                                                                       AS [Work order expected end date key]
    , wof.ExpectedStartDateKey                                                                                     AS [Work order expected start date key]
    , wof.ScheduleEndDateKey                                                                                       AS [Work order schedule end date key]
    , wof.ScheduleStartDateKey                                                                                     AS [Work order schedule start date key]
    , wof.WorkerResponsibleKey                                                                                     AS [Worker responsible key]
    , wof.WorkerScheduledKey                                                                                       AS [Worker schedule key]
    , c.DateKey                                                                                                    AS [Work order create date key]
    , CAST(1 AS INT)                                                                                               AS [Work orders]
    , CASE WHEN wof.ActualEndDateKey > 19000101 THEN wof.WorkOrderProcessDays END                                  AS [Work order process days]
    -- , DATEDIFF (DAY, wo.CreateDate, ISNULL (NULLIF(s.D365ExportStartDateTime, '1900-01-01'), GETUTCDATE ()))     AS [Work order age]
    , DATEDIFF (DAY, wo.CreateDate, GETUTCDATE ()) AS [Work order age]
    , CASE WHEN wo.ScheduleStartDate = '1900-01-01'
          THEN NULL
          ELSE
          CASE WHEN wo.ActualStartDate = '1900-01-01'
                THEN NULLIF(DATEDIFF (
                                DAY, wo.ScheduleStartDate, GETUTCDATE ()), 0)
                ELSE NULLIF(DATEDIFF (DAY, wo.ScheduleStartDate, wo.ActualStartDate), 0)END END                     AS [Overdue schedule start days]
  FROM {{ ref("workorder_fact") }}                   wof
INNER JOIN {{ ref("workorder") }}                   wo 
    ON wo.WorkOrderKey = wof.WorkOrderKey
  LEFT JOIN {{ ref('date') }}                        c
    ON c.Date          = wo.CreateDate;
