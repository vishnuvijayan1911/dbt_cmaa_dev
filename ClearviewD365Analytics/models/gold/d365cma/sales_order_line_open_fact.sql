{{ config(materialized='view', schema='gold', alias="Sales order line (open) fact") }}

SELECT  f.SalesOrderLineKey                                                                                              AS [Sales order line key]
    , CAST(1 AS INT)                                                                                                   AS [Sales order line count]
    , f.SalesOrderKey                                                                                                  AS [Sales order key]
    , f.BOMParentProductKey                                                                                            AS [BOM parent product key]
    , f.CustomerKey                                                                                                    AS [Customer key]
    , f.DeliveryAddressKey                                                                                             AS [Delivery address key]
    , f.InvoiceCustomerKey                                                                                             AS [Invoice customer key]
    , f.LegalEntityKey                                                                                                 AS [Legal entity key]
    , f.LotKey                                                                                                         AS [Lot key]
    , f.OnTimeShipStatusKey                                                                                            AS [On-time ship status key]
    , f.OrderDateKey                                                                                                   AS [Order date key]
    , f.ProductKey                                                                                                     AS [Product key] 
    , f.ReturnReasonKey                                                                                                AS [Return reason key]
    , f.ReservedDateKey                                                                                                AS [Reserved date key]
    , f.SalesCategoryKey                                                                                               AS [Sales category key]
    , f.SalesPersonKey                                                                                                 AS [Sales person key]
    , f.ShipDateActualKey                                                                                              AS [Ship date actual key]
    , f.ShipDateDueKey                                                                                                 AS [Ship date due key]
    , f.WarehouseKey                                                                                                   AS [Warehouse key]
    , f.BaseAmount                                                                                                     AS [Base amount]
    , f.BaseUnitPrice                                                                                                  AS [Order base unit price]
    , f.BaseUnitPrice_TransCur                                                                                         AS [Order base unit price in trans currency]
    , f.DaysLateTillDue                                                                                                AS [Days late / till due]
    , CASE WHEN st1.SalesTypeID = 4
            THEN NULL
            ELSE CASE WHEN ots.OnTimeShipStatusID = 3 THEN f.DaysLateTillDue ELSE NULL END END                          AS [Days ship late]
    , CASE WHEN st1.SalesTypeID = 4 THEN NULL ELSE CASE WHEN ots.OnTimeShipStatusID IN ( 2, 3 ) THEN 1 ELSE 0 END END  AS [Late order lines]
    , CASE WHEN st1.SalesTypeID = 4 THEN NULL ELSE CASE WHEN ots.OnTimeShipStatusID IN ( 2 ) THEN 1 ELSE 0 END END     AS [Past due order lines]
    , CASE WHEN st1.SalesTypeID = 4 THEN NULL ELSE
                                              CASE WHEN ots.OnTimeShipStatusID IN ( 1, 4, 5, 6 ) THEN 1 ELSE 0 END END AS [On-time order lines]
    , CASE WHEN st1.SalesTypeID = 4 THEN NULL ELSE CASE WHEN ots.OnTimeShipStatusID IN ( 3 ) THEN 1 ELSE 0 END END     AS [Ship late order lines]
    , ISNULL(f.NonBillableCharge, 0)                                                                                   AS [Order non-billable charges]
    , f.OpenLineCount                                                                                                  AS [Open lines]
    , f.OpenQuantity_SalesUOM                                                                                          AS [Open quantity]
      , f.OpenQuantity_LB * 1 AS [Open LB], f.OpenQuantity_LB * 0.01 AS [Open CWT], f.OpenQuantity_LB * 0.0005 AS [Open TON]
      , f.OpenQuantity_PC * 1 AS [Open PC]
      , f.DiscountAmount                                                                                                 AS [Order discount]
    , f.OrderLineCount                                                                                                 AS [Order lines]
    , f.NetAmount                                                                                                      AS [Order net amount]
    , f.OrderedQuantity_SalesUOM                                                                                       AS [Order quantity]
    , f.OrderedQuantity_LB * 1 AS [Order LB], f.OrderedQuantity_LB * 0.01 AS [Order CWT], f.OrderedQuantity_LB * 0.0005 AS [Order TON]
      , f.OrderedQuantity_PC * 1 AS [Order PC]
      , f.OrderedQuantity_FT * 1 AS [Order FT], f.OrderedQuantity_FT * 12 AS [Order IN]
    , ISNULL(f.CustomerCharge, 0)                                                                                      AS [Order total charges]
    , f.PriceUnit                                                                                                      AS [Order price unit]
    , NULLIF(f.PhysicalReservedQuantity_SalesUOM, 0)                                                                   AS [Physical reserved quantity]
      , NULLIF(f.PhysicalReservedQuantity_LB, 0) * 1 AS [Physical reserved LB], NULLIF(f.PhysicalReservedQuantity_LB, 0) * 0.01 AS [Physical reserved CWT], NULLIF(f.PhysicalReservedQuantity_LB, 0) * 0.0005 AS [Physical reserved TON]
      , NULLIF(f.PhysicalReservedQuantity_PC, 0) * 1 AS [Physical reserved PC]
  , NULLIF(f.RemainingAmount, 0)                                                                                     AS [Ship remain amount]
    , NULLIF(f.RemainingQuantity_SalesUOM, 0)                                                                          AS [Ship remain quantity]
      , NULLIF(f.RemainingQuantity_LB, 0) * 1 AS [Ship remain LB], NULLIF(f.RemainingQuantity_LB, 0) * 0.01 AS [Ship remain CWT], NULLIF(f.RemainingQuantity_LB, 0) * 0.0005 AS [Ship remain TON]
      , NULLIF(f.RemainingQuantity_PC, 0) * 1 AS [Ship remain PC]
    , f.ReturnLineCount                                                                                                AS [Return lines]
    , NULLIF(f.ShippedAmount, 0)                                                                                       AS [Ship amount]
    , NULLIF(f.ShippedQuantity_SalesUOM, 0)                                                                            AS [Ship quantity]
    , NULLIF(f.ShippedQuantity_LB, 0) * 1 AS [Ship LB], NULLIF(f.ShippedQuantity_LB, 0) * 0.01 AS [Ship CWT], NULLIF(f.ShippedQuantity_LB, 0) * 0.0005 AS [Ship TON]
      , NULLIF(f.ShippedQuantity_PC, 0) * 1 AS [Ship PC]
    , f.TotalUnitPrice                                                                                                 AS [Order total unit price]
    , f.TotalUnitPrice_TransCur                                                                                        AS [Order total unit price in trans currency]
  FROM {{ ref("SalesOrderLine_Fact") }}     f
  LEFT JOIN {{ ref("OnTimeShipStatus") }}   ots
    ON ots.OnTimeShipStatusKey    = f.OnTimeShipStatusKey
  LEFT JOIN {{ ref("SalesStatus") }}        ss1 
    ON ss1.SalesStatusKey         = f.SalesLineStatusKey
  LEFT JOIN {{ ref("SalesStatus") }}        ss2 
    ON ss2.SalesStatusKey         = f.SalesStatusKey
  LEFT JOIN {{ ref("SalesType") }}          st1
    ON st1.SalesTypeKey           = f.SalesTypeKey
    WHERE f.OpenLineCount = 1;
