{{ config(materialized='view', schema='gold', alias="Sales order line (open)") }}

SELECT 
    t.SalesOrderLineKey                                                       AS [Sales order line key]
    , NULLIF(t.SalesOrderID, '')                                                AS [Sales order #]
    , NULLIF(t.LineNumber, '')                                                  AS [Line #]
    , NULLIF(F.CustomerPO, '')                                                  AS [Customer PO]
    , NULLIF(F.CustomerReference, '')                                           AS [Customer reference]
    , NULLIF(t.CustomerPartNumber, '')                                          AS [Customer part number]
    , NULLIF(dm.DeliveryModeID, '')                                             AS [Delivery mode]
    , NULLIF(dm.DeliveryMode, '')                                               AS [Delivery mode name]
    , NULLIF(dt.DeliveryTermID, '')                                             AS [Delivery term]
    , NULLIF(dt.DeliveryTerm, '')                                               AS [Delivery term name]
    , NULLIF(ds.DocumentStatus, '')                                             AS [Document status]
    , NULLIF(ots.OnTimeShipStatus, '')                                          AS [On-time ship status]
    , NULLIF(ots.OnTimeStatus, '')                                              AS [On-time status]
    , NULLIF(ss1.SalesStatus, '')                                               AS [Order line status]
    , NULLIF(ss2.SalesStatus, '')                                               AS [Order status]
    , NULLIF(st.SalesType, '')                                                  AS [Order sales type]
    , NULLIF(pat.PaymentTermID, '')                                             AS [Payment term]
    , NULLIF(pat.PaymentTerm, '')                                               AS [Payment term name]
    , NULLIF(u1.UOM, '')                                                        AS [Pricing UOM]
    , NULLIF(F.ReturnItemID, '')                                                AS [Return item #]
    , NULLIF(rr.ReturnReason, '')                                               AS [Return reason]
    , NULLIF(rs.ReturnStatus, '')                                               AS [Return status]
    , NULLIF(sa.SalesAgreementID, '')                                           AS [Sales agreement #]
    , NULLIF(sp.SalesPerson, '')                                                AS [Sales person]
    , NULLIF(e4.EmployeeName, '')                                               AS [Sales taker]
    , NULLIF(u2.UOM, '')                                                        AS [Sales UOM]
    , NULLIF(ots.ShipStatus, '')                                                AS [Ship status]
    , NULLIF(tg.TaxGroup, '')                                                   AS [Tax group]
    , NULLIF(C.CurrencyID, '')                                                  AS [Trans currency]
    , F.DaysLateTillDue                                                         AS [Days late / till due]
    , CASE WHEN ots.OnTimeShipStatusID = 3 THEN F.DaysLateTillDue ELSE NULL END AS [Days ship late]
    , CASE WHEN ots.OnTimeShipStatusID IN ( 2, 3 ) THEN 1 ELSE 0 END            AS [Late order lines]
    , CASE WHEN ots.OnTimeShipStatusID IN ( 4, 5 ) THEN 1 ELSE 0 END            AS [On-time order lines]
    , CASE WHEN ots.OnTimeShipStatusID IN ( 3 ) THEN 1 ELSE 0 END               AS [Ship late order lines]
    , F.OpenLineCount                                                           AS [Open lines]
    , F.OrderLineCount                                                          AS [Order lines]
    , F.ReturnLineCount                                                         AS [Return lines]
    , NULLIF(dd1.Date, '1/1/1900')                                              AS [Created date]
    , NULLIF(dd2.Date, '1/1/1900')                                              AS [Ship date actual]
    , NULLIF(dd3.Date, '1/1/1900')                                              AS [Ship date confirmed]
    , NULLIF(dd4.Date, '1/1/1900')                                              AS [Ship date requested]
    , NULLIF(dd5.Date, '1/1/1900')                                              AS [Ship due date]
    , F.OrderedQuantity_SalesUOM                                                AS [Order quantity]
      , F.OrderedQuantity_LB * 1 AS [Order LB], F.OrderedQuantity_LB * 0.01 AS [Order CWT], F.OrderedQuantity_LB * 0.0005 AS [Order TON]
      , F.OrderedQuantity_PC * 1 AS [Order PC]
    , NULLIF(t.RMANumber,'')                                                    AS [RMA #]
  FROM {{ ref("salesorderline") }}           t 
INNER JOIN {{ ref("salesorderline_fact") }} F 
    ON F.SalesOrderLineKey      = t.SalesOrderLineKey
  LEFT JOIN {{ ref("currency") }}            C 
    ON C.CurrencyKey            = F.CurrencyKey
  LEFT JOIN {{ ref("deliverymode") }}        dm 
    ON dm.DeliveryModeKey       = F.DeliveryModeKey
  LEFT JOIN {{ ref("deliveryterm") }}        dt 
    ON dt.DeliveryTermKey       = F.DeliveryTermKey
  LEFT JOIN {{ ref("documentstatus") }}      ds 
    ON ds.DocumentStatusKey     = F.DocumentStatusKey
  LEFT JOIN {{ ref("paymentterm") }}         pat 
    ON pat.PaymentTermKey       = F.PaymentTermKey
  LEFT JOIN {{ ref("uom") }}                 u1 
    ON u1.UOMKey                = F.PricingUOMKey
  LEFT JOIN {{ ref("returnstatus") }}        rs 
    ON rs.ReturnStatusKey       = F.ReturnStatusKey
  LEFT JOIN {{ ref("returnreason") }}        rr 
    ON rr.ReturnReasonKey       = F.ReturnReasonKey
  LEFT JOIN {{ ref("salesstatus") }}         ss1 
    ON ss1.SalesStatusKey       = F.SalesLineStatusKey
  LEFT JOIN {{ ref("salesstatus") }}         ss2 
    ON ss2.SalesStatusKey       = F.SalesStatusKey
  LEFT JOIN {{ ref("salestype") }}           st 
    ON st.SalesTypeKey          = F.SalesTypeKey
  LEFT JOIN {{ ref("uom") }}                 u2 
    ON u2.UOMKey                = F.SalesUOMKey
  LEFT JOIN {{ ref("taxgroup") }}            tg 
    ON tg.TaxGroupKey           = F.TaxGroupKey
  LEFT JOIN {{ ref('date') }}                dd1 
    ON dd1.DateKey              = F.OrderDateKey
  LEFT JOIN {{ ref('date') }}                dd2 
    ON dd2.DateKey              = F.ShipDateActualKey
  LEFT JOIN {{ ref('date') }}                dd3 
    ON dd3.DateKey              = F.ShipDateConfirmedKey
  LEFT JOIN {{ ref('date') }}                dd4 
    ON dd4.DateKey              = F.ShipDateRequestedKey
  LEFT JOIN {{ ref('date') }}                dd5 
    ON dd5.DateKey              = F.ShipDateDueKey
  LEFT JOIN {{ ref("salesperson") }}         sp 
    ON sp.SalesPersonKey        = F.SalesPersonKey
  LEFT JOIN {{ ref("employee") }}            e4 
    ON e4.EmployeeKey           = F.SalesTakerKey
  LEFT JOIN {{ ref("ontimeshipstatus") }}    ots 
    ON ots.OnTimeShipStatusKey  = F.OnTimeShipStatusKey
  LEFT JOIN {{ ref("salesagreementline") }}  sa 
    ON sa.SalesAgreementLineKey = F.SalesAgreementLineKey
    WHERE F.OpenLineCount = 1;
