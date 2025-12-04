{{ config(materialized='view', schema='gold', alias="Production route") }}

SELECT  t.ProductionRouteKey                                AS [Production route key]
    , NULLIF(t.OperationFinished, '')                     AS [Operation finished]
    , NULLIF(t.OperationNumber, '')                       AS [Operation #]
    , NULLIF(pro.OperationID, '')                         AS [Operation]
    , NULLIF(pro.Operation, '')                           AS [Operation name]
    , CASE WHEN t.IsOSP = 1 THEN 'OSP' ELSE 'Not OSP' END AS [OSP]
    , NULLIF(t.ProcessJobID, '')                          AS [Process job #]
    , NULLIF(prg.ProductionRouteGroup, '')                AS [Production route group]
    , NULLIF(t.QuantityCostCategory, '')                  AS [Quantity cost category]
    , NULLIF(t.RequirementType, '')                       AS [Requirement type]
    , NULLIF(t.RunCostCategory, '')                       AS [Run cost category]
    , NULLIF(t.SetupCostCategory, '')                     AS [Setup cost category]
    , NULLIF(t.SubContractItem, '')                       AS [Sub-contract item]
    , NULLIF(UPPER(t.SubContractUOM), '')                 AS [Sub-contract UOM]
    , NULLIF(dd.Date, '1/1/1900')                         AS [Schedule start date]
    , NULLIF(t.ScheduleStartTime, '00:00:00')             AS [Schedule start time]
    , NULLIF(dd1.Date, '1/1/1900')                        AS [Schedule end date]
    , NULLIF(t.ScheduleEndTime, '00:00:00')               AS [Schedule end time]
  FROM {{ ref("d365cma_productionroute_d") }}               t 
INNER JOIN {{ ref("d365cma_productionroute_f") }}     prf 
    ON prf.ProductionRouteKey          = t.ProductionRouteKey
  LEFT JOIN {{ ref('d365cma_date_d') }}                     dd 
    ON dd.DateKey                      = prf.ScheduleStartDateKey
  LEFT JOIN {{ ref('d365cma_date_d') }}                     dd1 
    ON dd1.DateKey                     = prf.ScheduleEndDateKey
  LEFT JOIN {{ ref("d365cma_productionroutegroup_d") }}     prg 
    ON prg.ProductionRouteGroupKey     = prf.ProductionRouteGroupKey
  LEFT JOIN {{ ref("d365cma_productionrouteoperation_d") }} pro 
    ON pro.ProductionRouteOperationKey = prf.ProductionRouteOperationKey;
