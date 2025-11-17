{{ config(materialized='view', schema='gold', alias="Production") }}

SELECT  t.ProductionKey                                                                                     AS [Production key]
    , f.LegalEntityKey                                                                                    AS [Legal entity key]
    , f.WarehouseKey                                                                                      AS [Warehouse key]
    , f.ProductKey                                                                                        AS [Product key]
    , f.OrderCreatedDateKey                                                                               AS [Order created date key]
    , CASE WHEN dps.ProductionStatusID = 5 THEN f.ReportAsFinishedDateKey ELSE f.ProductionEndDateKey END AS [Order end date key]
    , NULLIF(t.BOMID, '')                                                                                 AS [BOM #]
    , NULLIF(t.CompletedStatus, '')                                                                       AS [Completed status]
    , NULLIF(t.CustomerReferenceNumber, '')                                                               AS [Customer reference]
    , NULLIF(dpst.InventoryReferenceType, '')                                                             AS [Inventory reference type]
    , NULLIF(t.OnTimeProductionStatus, '')                                                                AS [On-time production status]
    , NULLIF(t.OnTimeStatus, '')                                                                          AS [On-time status]
    , NULLIF(dpg.ProductionGroup, '')                                                                     AS [Production group]
    , NULLIF(t.ProductionID, '')                                                                          AS [Production #]
    , NULLIF(dpp.ProductionPool, '')                                                                      AS [Production pool]
    , NULLIF(dprs.ProductionRemainingStatus, '')                                                          AS [Production remaining status]
    , NULLIF(dpss.ProductionScheduleStatus, '')                                                           AS [Production schedule status]
    , NULLIF(dps.ProductionStatus, '')                                                                    AS [Production status]
    , ISNULL(NULLIF(dps.ProductionStatusID, ''), 0)                                                       AS [Production status ID]
    , NULLIF(dpt.ProductionType, '')                                                                      AS [Production type]
    , NULLIF(du1.UOM, '')                                                                                 AS [Production UOM]
    , NULLIF(rp.ProductionID, '')                                                                         AS [Reference production #]
    , NULLIF(du.UOM, '')                                                                                  AS [RAF UOM]
    , NULLIF(t.RouteID, '')                                                                               AS [Route #]
    , NULLIF(t.FirstTransDate, '1/1/1900')                                                                AS [First trans date]
    , NULLIF(dd.Date, '1/1/1900')                                                                         AS [Due date]
    , NULLIF(t.LastTransDate, '1/1/1900')                                                                 AS [Last trans date]
    , NULLIF(t.PlannedDeliveryDate, '1/1/1900')                                                           AS [Planned delivery date]
    , NULLIF(dd1.Date, '1/1/1900')                                                                        AS [Production start date]
    , NULLIF(CASE WHEN dps.ProductionStatusID = 5 THEN dd2.Date ELSE dd5.Date END, '1/1/1900')            AS [Production end date]
    , NULLIF(dd2.Date, '1/1/1900')                                                                        AS [RAF date]
    , CAST(NULLIF(dd3.Date, '1/1/1900') AS DATE)                                                          AS [Schedule start date]
    , CAST(NULLIF(dd4.Date, '1/1/1900') AS DATE)                                                          AS [Schedule end date]
    , NULLIF(t.ScheduleStartTime, '1/1/1900')                                                             AS [Schedule start time]
    , NULLIF(t.ScheduleEndTime, '1/1/1900')                                                               AS [Schedule end time]
  FROM {{ ref("production_d") }}                     t 
  LEFT JOIN {{ ref("production_f") }}           f 
    ON f.ProductionKey                   = t.ProductionKey
INNER JOIN {{ ref("uom_d") }}                       du 
    ON du.UOMKey                         = f.ReportAsFinishedUOMKey
INNER JOIN {{ ref("uom_d") }}                       du1 
    ON du1.UOMKey                        = f.ProductionUOMKey
INNER JOIN {{ ref("productiongroup_d") }}           dpg 
    ON dpg.ProductionGroupKey            = f.ProductionGroupKey
INNER JOIN {{ ref("productionpool_d") }}            dpp 
    ON dpp.ProductionPoolKey             = f.ProductionPoolKey
INNER JOIN {{ ref("productionstatus_d") }}          dps 
    ON dps.ProductionStatusKey           = f.ProductionStatusKey
INNER JOIN {{ ref("productiontype_d") }}            dpt 
    ON dpt.ProductionTypeKey             = f.ProductionTypeKey
INNER JOIN {{ ref("productionschedulestatus_d") }}  dpss 
    ON dpss.ProductionScheduleStatusKey  = f.ProductionScheduleStatusKey
INNER JOIN {{ ref("productionremainingstatus_d") }} dprs 
    ON dprs.ProductionRemainingStatusKey = f.ProductionRemainingStatusKey
INNER JOIN {{ ref("inventoryreferencetype_d") }}    dpst 
    ON dpst.InventoryReferenceTypeKey    = f.InventoryReferenceTypeKey
INNER JOIN {{ ref("production_d") }}                rp 
    ON rp.ProductionKey                  = f.ReferenceProductionKey
  LEFT JOIN {{ ref('date_d') }}                      dd 
    ON dd.DateKey                        = f.DueDateKey
  LEFT JOIN {{ ref('date_d') }}                      dd1 
    ON dd1.DateKey                       = f.ProductionStartDateKey
  LEFT JOIN {{ ref('date_d') }}                      dd2 
    ON dd2.DateKey                       = f.ReportAsFinishedDateKey
  LEFT JOIN {{ ref('date_d') }}                      dd3 
    ON dd3.DateKey                       = f.ScheduleStartDateKey
  LEFT JOIN {{ ref('date_d') }}                      dd4 
    ON dd4.DateKey                       = f.ScheduleEndDateKey
  LEFT JOIN {{ ref('date_d') }}                      dd5 
    ON dd5.DateKey                       = f.ProductionEndDateKey;
