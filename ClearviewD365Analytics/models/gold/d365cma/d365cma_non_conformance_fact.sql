{{ config(materialized='view', schema='gold', alias="Non conformance fact") }}

SELECT t.NonConformanceKey       AS [Non-conformance key]
     , t.InventorySiteKey        AS [Inventory site key]
     , t.LegalEntityKey          AS [Legal entity key]
     , t.NonConformanceDateKey   AS [Non-conformance date key]
     , t.ProductKey              AS [Product key]
     , t.ProductionKey           AS [Production key]
     , t.WarehouseKey            AS [Warehouse key]
     , CAST(1 AS INT)            AS [Non-conformance count]
     , t.TestDefectQuantity      AS [Test defect quantity]
     , t.TestDefectQuantity_FT * 1 AS [Test defect FT], t.TestDefectQuantity_FT * 12 AS [Test defect IN]
	   , t.TestDefectQuantity_LB * 1 AS [Test defect LB], t.TestDefectQuantity_LB * 0.01 AS [Test defect CWT], t.TestDefectQuantity_LB * 0.0005 AS [Test defect TON]
     , t.TestDefectQuantity_PC * 1 AS [Test defect PC]
     , 0 AS [Test defect SQIN]
  FROM {{ ref("d365cma_nonconformance_f") }} t;
