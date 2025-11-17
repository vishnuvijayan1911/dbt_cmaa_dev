{{ config(materialized='view', schema='gold', alias="Production fact") }}

WITH cte
  AS (
    SELECT  pt.ProductionKey
          , SUM(pt.IssueQuantity_LB) * 1 AS [Issue LB], SUM(pt.IssueQuantity_LB) * 0.01 AS [Issue CWT], SUM(pt.IssueQuantity_LB) * 0.0005 AS [Issue TON]
      FROM {{ ref("ProductionPickListJournal_Fact") }} pt 
      LEFT JOIN {{ ref("Production") }}                f 
        ON f.ProductionKey = pt.ProductionKey
      GROUP BY pt.ProductionKey)
  , cte1
  AS (SELECT  pt.ProductionKey
          , SUM(pt.ActualRunQuantity)     AS [Actual run quantity]
            ,SUM(pt.ActualRunQuantity_LB) * 1 AS [Actual run LB], SUM(pt.ActualRunQuantity_LB) * 0.01 AS [Actual run CWT], SUM(pt.ActualRunQuantity_LB) * 0.0005 AS [Actual run TON]
        FROM {{ ref("ProductionFinishedJournal_Fact") }} pt 
        LEFT JOIN {{ ref("Production") }}                f 
          ON f.ProductionKey = pt.ProductionKey
      GROUP BY pt.ProductionKey)
  , cte2
  AS (   
  SELECT ProductionKey
      ,SUM(CoByQuantity) AS CoByQuantity
      ,SUM(cobyquantity_pc) AS cobyquantity_pc
      ,SUM(cobyquantity_lb) AS cobyquantity_lb
  FROM {{ ref("ProductionCoProduct_Fact") }}
  GROUP BY ProductionKey)
SELECT  t.ProductionKey                                                                     AS [Production key]
    , t.DueDateKey                                                                        AS [Due date key]
    , t.OrderCreatedDateKey                                                               AS [Order created date key]
    , t.FinancialKey                                                                      AS [Financial key]
    , t.InventorySiteKey                                                                  AS [Inventory site key]
    , t.LegalEntityKey                                                                    AS [Legal entity key]
    , t.LotKey                                                                            AS [Lot key]
    , t.ProductionStartDateKey                                                            AS [Production start date key]
    , t.ProductionEndDateKey                                                              AS [Production end date key]
    , t.ProductKey                                                                        AS [Product key]
    , COALESCE(NULLIF(t.ScheduleStartDateKey, 19000101), t.OrderCreatedDateKey, 19000101) AS [Report date key]
    , t.ReferenceProductionKey                                                            AS [Reference production key]
    , t.ReportAsFinishedDateKey                                                           AS [RAF date key]
    , t.ScheduleEndDateKey                                                                AS [Schedule end date key]
    , t.ScheduleStartDateKey                                                              AS [Schedule start date key]
    , t.WarehouseKey                                                                      AS [Warehouse key]
    , t.WarehouseLocationKey                                                              AS [Warehouse location key]
    , t.ActualScrapQuantity                                                               AS [Actual scrap quantity]  
      ,t.ActualScrapQuantity_LB * 1 AS [Actual scrap LB], t.ActualScrapQuantity_LB * 0.01 AS [Actual scrap CWT], t.ActualScrapQuantity_LB * 0.0005 AS [Actual scrap TON]
      ,t.ActualScrapQuantity_PC * 1 AS [Actual scrap PC]

    , t.EstimatedScrapQuantity                                                            AS [Estimated scrap quantity]   
      ,t.EstimatedScrapQuantity_LB * 1 AS [Estimated scrap LB], t.EstimatedScrapQuantity_LB * 0.01 AS [Estimated scrap CWT], t.EstimatedScrapQuantity_LB * 0.0005 AS [Estimated scrap TON]
      ,t.EstimatedScrapQuantity_PC * 1 AS [Estimated scrap PC]

    , t.OrderedQuantity                                                                   AS [Order quantity]   
      ,CAST((CASE WHEN c2.ProductionKey IS NULL THEN t.OrderedQuantity_LB ELSE c2.cobyquantity_lb END) AS numeric(20,4)) * 1 AS [Order LB], CAST((CASE WHEN c2.ProductionKey IS NULL THEN t.OrderedQuantity_LB ELSE c2.cobyquantity_lb END) AS numeric(20,4)) * 0.01 AS [Order CWT], CAST((CASE WHEN c2.ProductionKey IS NULL THEN t.OrderedQuantity_LB ELSE c2.cobyquantity_lb END) AS numeric(20,4)) * 0.0005 AS [Order TON] --ADO374864: Pull conversion from Co-products level in case of Co Production order
      ,CAST((CASE WHEN c2.ProductionKey IS NULL THEN t.OrderedQuantity_PC ELSE c2.cobyquantity_pc END) AS numeric(20,4)) * 1 AS [Order PC] --ADO374864: Pull conversion from Co-products level in case of Co Production order

      ,CAST(t.OverUnderProduced_LB    AS numeric(32,6)) * 1 AS [Over/under produced LB], CAST(t.OverUnderProduced_LB    AS numeric(32,6)) * 0.01 AS [Over/under produced CWT], CAST(t.OverUnderProduced_LB    AS numeric(32,6)) * 0.0005 AS [Over/under produced TON]
    , t.RAFCost                                                                           AS [RAF cost]
    , t.ProductionOrderWeight															                               AS [Production Order Weight]
    , t.StandardQuantity                                                                  AS [Standard quantity]
      , CAST(t.StandardQuantity_LB AS numeric(32,6)) * 1 AS [Standard LB], CAST(t.StandardQuantity_LB AS numeric(32,6)) * 0.01 AS [Standard CWT], CAST(t.StandardQuantity_LB AS numeric(32,6)) * 0.0005 AS [Standard TON]
      , CAST(t.StandardQuantity_PC AS numeric(32,6)) * 1 AS [Standard PC]
    , CAST(t.WIPQuantity_LB AS  numeric(20,4)) * 1 AS [WIP LB], CAST(t.WIPQuantity_LB AS  numeric(20,4)) * 0.01 AS [WIP CWT], CAST(t.WIPQuantity_LB AS  numeric(20,4)) * 0.0005 AS [WIP TON]
      , CAST(t.WIPQuantity_PC AS  numeric(20,4)) * 1 AS [WIP PC]
    , CASE WHEN f.CompletedStatus = 'Completed' THEN 1 ELSE 0 END                         AS [Completed orders]
    , CASE WHEN f.OnTimeProductionStatus = 'Completed on-time' THEN 1 ELSE 0 END          AS [Completed on-time orders]
    , CASE WHEN f.OnTimeProductionStatus = 'Completed late' THEN 1 ELSE 0 END             AS [Completed late orders]
    , CASE WHEN dps.ProductionStatus = 'Started' THEN 1 ELSE 0 END                        AS [Running orders]
    , CASE WHEN f.OnTimeStatus = 'Late' THEN 1 ELSE 0 END                                 AS [Late orders]
    , CASE WHEN f.OnTimeStatus = 'On-time' THEN 1 ELSE 0 END                              AS [On-time orders]
    , ABS(DATEDIFF(DAY, f.LastTransDate, f.FirstTransDate))                               AS [Processing days]
    , 1                                                                                   AS [Production orders]
      , CAST(c.[Issue LB] AS numeric(38,4)) * 1 AS [Issue LB], CAST(c.[Issue LB] AS numeric(38,4)) * 0.01 AS [Issue CWT], CAST(c.[Issue LB] AS numeric(38,4)) * 0.0005 AS [Issue TON]
    , CAST(c1.[Actual run quantity]   AS  numeric(38,4))                                                           AS [Actual run quantity]
    , CAST(c1.[Actual run LB] AS  numeric(38,4)) * 1 AS [Actual run LB], CAST(c1.[Actual run LB] AS  numeric(38,4)) * 0.01 AS [Actual run CWT], CAST(c1.[Actual run LB] AS  numeric(38,4)) * 0.0005 AS [Actual run TON]
    , d.Date                                                                              AS [Order date]
  FROM {{ ref("Production_Fact") }}       t 
  LEFT JOIN {{ ref("Production") }}       f 
    ON f.ProductionKey         = t.ProductionKey
  LEFT JOIN {{ ref("ProductionStatus") }} dps 
    ON dps.ProductionStatusKey = t.ProductionStatusKey
  LEFT JOIN cte                  c 
    ON c.ProductionKey         = t.ProductionKey
  LEFT JOIN cte1                 c1 
    ON c1.ProductionKey        = t.ProductionKey
  LEFT JOIN {{ ref('date') }}             d
    ON d.DateKey               = t.OrderCreatedDateKey
  LEFT JOIN cte2                 c2 
    ON c2.ProductionKey        = t.ProductionKey;
