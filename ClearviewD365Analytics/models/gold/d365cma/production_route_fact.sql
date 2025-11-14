{{ config(materialized='view', schema='gold', alias="Production route fact") }}

WITH cte1
  AS (
    SELECT  pt.ProductionKey
          , SUM(pt.ActualRunQuantity)             AS [Actual run quantity]
            , SUM(pt.ActualRunQuantity_LB) * 1 AS [Actual run LB], SUM(pt.ActualRunQuantity_LB) * 0.01 AS [Actual run CWT], SUM(pt.ActualRunQuantity_LB) * 0.0005 AS [Actual run TON]
      FROM {{ ref("ProductionFinishedJournal_Fact") }} pt 
      LEFT JOIN {{ ref("Production") }}                f 
        ON f.ProductionKey = pt.ProductionKey
      GROUP BY pt.ProductionKey)
SELECT  t.ProductionRouteKey                                                                 AS [Production route key]
    , t.ProductionKey                                                                      AS [Production key]
    , t.FinancialKey                                                                       AS [Financial key]
    , t.LegalEntityKey                                                                     AS [Legal entity key]
    , t.ProductionResourceKey                                                              AS [Production resource key]
    , t.ScheduleStartDateKey                                                               AS [Schedule start date key]
    , t.ScheduleEndDateKey                                                                 AS [Schedule end date key]
    , t.VendorKey                                                                          AS [Vendor key]
    , t.ActualRunHours                                                                     AS [Actual run hours]
    , t.ActualSetUpHours                                                                   AS [Actual set up hours]
    , t.EstimatedRunHours                                                                  AS [Estimated run hours]
    , t.EstimatedSetupHours                                                                AS [Estimated setup hours]
    , t.HourlyRate                                                                         AS [Hourly rate]
    , t.QuantityPrice                                                                      AS [Quantity price]
    , t.ResourceQuantity                                                                   AS [Resource quantity]
    , 1                                                                                    AS [Route count]
    , CASE WHEN COALESCE(t.EstimatedRunHours, 0) <> 0
            AND COALESCE(p.OrderedQuantity, 0) <> 0
            THEN ((t.EstimatedRunHours / p.OrderedQuantity) * c1.[Actual run quantity]) END AS [Estimated run hours (adjusted)]
  FROM {{ ref("ProductionRoute_Fact") }} t 
INNER JOIN {{ ref("Production_Fact") }} p 
    ON p.ProductionKey  = t.ProductionKey
  LEFT JOIN cte1                c1
    ON c1.ProductionKey = p.ProductionKey;
