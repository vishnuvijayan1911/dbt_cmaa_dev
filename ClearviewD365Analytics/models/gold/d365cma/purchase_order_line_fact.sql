{{ config(materialized='view', schema='gold', alias="Purchase order line fact") }}

SELECT  t.PurchaseOrderLineKey                                                                                              AS [Purchase order line key]
  , t.PurchaseOrderKey                                                                                                  AS [Purchase order key]
  , t.DeliveryAddressKey                                                                                                AS [Delivery address key]
  , t.DeliveryDateActualKey                                                                                             AS [Delivery date actual key]
  , t.DeliveryDateKey                                                                                                   AS [Delivery date due key]
  , t.InvoiceVendorKey                                                                                                  AS [Invoice vendor key]
  , t.LegalEntityKey                                                                                                    AS [Legal entity key]
  , t.LotKey                                                                                                            AS [Lot key]
  , t.OnTimeDeliveryStatusKey                                                                                           AS [On-time delivery status key]
  , t.OrderDateKey                                                                                                      AS [Order date key]
  , t.ProcurementCategoryKey                                                                                            AS [Procurement category key]
  , t.ProductKey							                                                                               AS [Product key]
  , t.VendorKey                                                                                                         AS [Vendor key]
  , t.WarehouseKey                                                                                                      AS [Warehouse key]
  , t.BaseAmount                                                                                                        AS [Base amount]
  , t.BaseUnitPrice                                                                                                     AS [Order base unit price]
  , t.BaseUnitPrice_TransCur                                                                                            AS [Order base unit price in trans currency]
  , t.DaysLateTillDue                                                                                                   AS [Days late / till due]
  , CASE WHEN pt1.PurchaseTypeID = 4
          THEN NULL
          ELSE CASE WHEN ots.OnTimeDeliveryStatusID = 3 THEN t.DaysLateTillDue ELSE NULL END END                         AS [Days receive late]
  , CASE WHEN pt1.PurchaseTypeID = 4 THEN NULL ELSE
                                                CASE WHEN ots.OnTimeDeliveryStatusID IN ( 2, 3 ) THEN 1 ELSE 0 END END   AS [Late order lines]
  , CASE WHEN pt1.PurchaseTypeID = 4 THEN NULL ELSE
                                                CASE WHEN ots.OnTimeDeliveryStatusID IN ( 4, 5 ) THEN 1 ELSE 0 END END   AS [On-time order lines]
  , CASE WHEN pt1.PurchaseTypeID = 4 THEN NULL ELSE CASE WHEN ots.OnTimeDeliveryStatusID IN ( 2 ) THEN 1 ELSE 0 END END AS [Past due order lines]
  , CASE WHEN pt1.PurchaseTypeID = 4 THEN NULL ELSE CASE WHEN ots.OnTimeDeliveryStatusID IN ( 3 ) THEN 1 ELSE 0 END END AS [Receive late order lines]
  , ISNULL(t.NonBillableCharge, 0)                                                                                      AS [Order non-billable charges]
  , t.OpenLineCount                                                                                                     AS [Open lines]
  , t.DiscountAmount                                                                                                    AS [Order discount]
  , t.NetAmount                                                                                                         AS [Order net amount]
  , t.OrderedQuantity_PurchUOM                                                                                          AS [Order quantity]
    ,t.OrderedQuantity_LB * 1 AS [Order LB], t.OrderedQuantity_LB * 0.01 AS [Order CWT], t.OrderedQuantity_LB * 0.0005 AS [Order TON]
    ,t.OrderedQuantity_FT * 1 AS [Order FT], t.OrderedQuantity_FT * 12 AS [Order IN]
    ,t.OrderedQuantity_PC * 1 AS [Order PC]
  , ISNULL(t.VendorCharge, 0)                                                                                           AS [Order total charges]
  , t.PriceUnit                                                                                                         AS [Order price unit]
  , t.PurchaseLineCount                                                                                                 AS [Order lines]
  , NULLIF(t.ReceivedAmount, 0)                                                                                         AS [Receive amount]
  , NULLIF(t.ReceivedQuantity_PurchUOM, 0)                                                                              AS [Receive quantity]
    ,NULLIF(t.ReceivedQuantity_LB, 0) * 1 AS [Receive LB], NULLIF(t.ReceivedQuantity_LB, 0) * 0.01 AS [Receive CWT], NULLIF(t.ReceivedQuantity_LB, 0) * 0.0005 AS [Receive TON]
    ,NULLIF(t.ReceivedQuantity_PC, 0) * 1 AS [Receive PC]
  , NULLIF(t.RemainingAmount, 0)                                                                                        AS [Receive remain amount]
  , NULLIF(t.RemainingQuantity_PurchUOM, 0)                                                                             AS [Receive remain quantity]
    ,NULLIF(t.RemainingQuantity_LB, 0) * 1 AS [Receive remain LB], NULLIF(t.RemainingQuantity_LB, 0) * 0.01 AS [Receive remain CWT], NULLIF(t.RemainingQuantity_LB, 0) * 0.0005 AS [Receive remain TON]
    ,NULLIF(t.RemainingQuantity_PC, 0) * 1 AS [Receive remain PC]
  , t.ReturnLineCount                                                                                                   AS [Return lines]
  , CAST(1 AS INT)                                                                                                      AS [Purchase order line count]
  , t.TotalUnitPrice                                                                                                    AS [Order total unit price]
  , t.TotalUnitPrice_TransCur                                                                                           AS [Order total unit price in trans currency]
FROM {{ ref("PurchaseOrderLine_Fact") }}    t 
JOIN {{ ref("PurchaseType") }}              st 
  ON st.PurchaseTypeKey          = t.PurchaseTypeKey
LEFT JOIN {{ ref("OnTimeDeliveryStatus") }} ots 
  ON ots.OnTimeDeliveryStatusKey = t.OnTimeDeliveryStatusKey
LEFT JOIN {{ ref("PurchaseStatus") }}       ps 
  ON ps.PurchaseStatusKey        = t.PurchaseLineStatusKey
LEFT JOIN {{ ref("PurchaseType") }}         pt1
  ON pt1.PurchaseTypeKey         = t.PurchaseTypeKey
LEFT JOIN {{ ref("PurchaseStatus") }}       ps2 
  ON ps2.PurchaseStatusKey       = t.PurchaseStatusKey
