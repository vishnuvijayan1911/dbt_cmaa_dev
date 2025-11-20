{{ config(materialized='table', tags=['silver'], alias='salesquoteline_fact') }}

-- Source file: cma/cma/layers/_base/_silver/salesquoteline_f/salesquoteline_f.py
-- Root method: SalesquotelineFact.get_detail_query [SalesQuoteLine_FactDetail]
-- Inlined methods: SalesquotelineFact.get_fact_stage_query [SalesQuoteLine_FactStage], SalesquotelineFact.get_main_query [SalesQuoteLine_FactDetailMain]
-- external_table_name: SalesQuoteLine_FactDetail
-- schema_name: temp

WITH
salesquoteline_factstage AS (
    SELECT DISTINCT
          ql.dataareaid                                                                                        AS LegalEntityID
        , ql.salescategory                                                                                     AS RecID_SC
        , ql.custaccount                                                                                       AS CustomerAccount
        , CAST(qt.quotationexpirydate AS DATE)                                                                 AS ExpirationDate
        , qt.invoiceaccount                                                                                    AS InvoiceAccount
        , CAST(CAST(ql.createddatetime AS DATETIME) AT TIME ZONE 'UTC' AT TIME ZONE le.TimeZone AS DATE)       AS QuoteDate
        , ql.defaultdimension                                                                                  AS DefaultDimension
        , ql.dlvmode                                                                                           AS DeliveryModeID
        , ql.dlvterm                                                                                           AS DeliveryTermID
        , qt.inventsiteid                                                                                      AS InventSiteID
        , ql.itemid                                                                                            AS ItemID
        , id.inventlocationid                                                                                  AS WarehouseID
        , ito.recid                                                                                           AS RecID_ITO
        , qt.paymmode                                                                                          AS PaymentModeID
        , qt.payment                                                                                           AS PaymentTermID
        , id.inventsizeid                                                                                      AS ProductWidth
        , id.inventcolorid                                                                                     AS ProductLength
        , id.inventstyleid                                                                                     AS ProductColor
        , it.product                                                                                           AS ProductID
        , id.configid                                                                                          AS ProductConfig
        , im.unitid                                                                                   AS InventoryUnit
        , ql.currencycode                                                                                      AS CurrencyID
        , ql.quotationstatus                                                                                   AS QuoteStatusID
        , ql.quotationtype                                                                                     AS QuoteTypeID
        , it.cmacostingunit                                                                            AS CostingUnit
        , ql.lineamount                                                                                        AS NetAmount_TransCur
        , ((CASE WHEN ql.salesprice = 0
                  THEN CASE WHEN ql.cmatotalamount = 0 THEN 0 ELSE ql.cmatotalamount / ISNULL(NULLIF(ql.salesqty, 0), 1) END
                  ELSE ql.salesprice END) * (CASE WHEN ql.salesqty = 0 THEN 1 ELSE ql.salesqty END)
            / (CASE WHEN ql.salesprice = 0 THEN 1 ELSE ISNULL(NULLIF(ql.priceunit, 0), 1) END)) - ql.lineamount AS DiscountAmount_TransCur
        , ql.cmatotalamount                                                                                    AS TotalAmount_TransCur
        , ql.salesqty                                                                                          AS OrderedQuantity_SalesUOM
        , ql.qtyordered                                                                                        AS OrderedQuantity
        , CAST(ql.receiptdaterequested AS DATE)                                                                AS ReceiptDateRequested
        , ql.remainsalesphysical                                                                               AS RemainingQuantity_SalesUOM
        , ql.remaininventphysical                                                                              AS RemainingQuantity
        , ql.priceunit                                                                                         AS PriceUnit
        , ql.salesunit                                                                                         AS SalesUOM
        , CAST(ql.shippingdaterequested AS DATE)                                                               AS ShipDateRequested
        , ql.salesprice                                                                                        AS BaseUnitPrice_TransCur
        , ql.cmatotalprice                                                                                     AS TotalUnitPrice_TransCur
        , qt.workersalestaker                                                                                  AS SalesTaker
        , ql.customerref                                                                                       AS CustomerReference
        , qt.workersalesresponsible                                                                            AS SalesPersonID
        , ql.modifieddatetime                                                                                 AS _SourceDate
        , 1                                                                                                    AS _SourceID
        , ql.recid                                                                                            AS _RecID
          , ql.cmapriceuom                                                                                       AS QuotePriceUOM
      FROM  {{ ref('salesquotationline') }}       ql
    INNER JOIN {{ ref('salesquotationtable') }} qt
        ON qt.dataareaid    = ql.dataareaid
      AND qt.quotationid    = ql.quotationid
    INNER JOIN {{ ref('legalentity_d') }}         le
        ON le.LegalEntityID  = ql.dataareaid
    INNER JOIN {{ ref('inventdim') }}           id
        ON id.dataareaid    = ql.dataareaid
      AND id.inventdimid    = ql.inventdimid
      LEFT JOIN  {{ ref('salestable') }}          st
        ON st.dataareaid    = qt.dataareaid
      AND st.salesid        = qt.salesidref
      LEFT JOIN {{ ref('inventtable') }}         it
        ON it.dataareaid    = ql.dataareaid
      AND it.itemid         = ql.itemid
      LEFT JOIN {{ ref('inventtablemodule') }}   im
        ON im.dataareaid    = it.dataareaid
      AND im.itemid         = it.itemid
      AND im.moduletype     = 0
      LEFT JOIN {{ ref('inventtransorigin') }}   ito
        ON ito.dataareaid   = ql.dataareaid
      AND ito.inventtransid = ql.inventtransid;
),
salesquoteline_factdetailmain AS (
    SELECT dql.SalesQuoteLineKey
        , le.LegalEntityKey             AS LegalEntityKey
        , fd.FinancialKey               AS FinancialKey
        , cc.CurrencyKey                AS CurrencyKey
        , dc1.CustomerKey               AS CustomerKey
        , ddm.DeliveryModeKey           AS DeliveryModeKey
        , ddt.DeliveryTermKey           AS DeliveryTermKey
        , dd1.DateKey                   AS ExpirationDateKey
        , dc2.CustomerKey               AS InvoiceCustomerKey
        , sp.SalesPersonKey             AS SalesPersonKey
        , it.lotkey                     AS LotKey
        , ds.InventorySiteKey           AS InventorySiteKey
        , ISNULL(dp.ProductKey, -1)     AS ProductKey
        , dpm.PaymentModeKey            AS PaymentModeKey
        , dpt.PaymentTermKey            AS PaymentTermKey
        , dd.DateKey                    AS QuoteDateKey
        , qs.QuoteStatusKey             AS QuoteStatusKey
        , qt.quotetypekey               AS QuoteTypeKey
        , dd2.DateKey                   AS ReceiptDateRequestedKey
        , dsc.SalesCategoryKey          AS SalesCategoryKey
        , de2.EmployeeKey               AS SalesTakerKey
        , dd3.DateKey                   AS ShipDateRequestedKey
        , dw.WarehouseKey               AS WarehouseKey
        , du.UOMKey                     AS InventoryUOMKey
        , du1.UOMKey                    AS SalesUOMKey
        , du2.UOMKey                    AS QuotePriceUOMKey
        , ts.DiscountAmount_TransCur    AS DiscountAmount_TransCur
        , ts.NetAmount_TransCur         AS NetAmount_TransCur
        , ts.OrderedQuantity_SalesUOM   AS OrderedQuantity_SalesUOM
        , ts.OrderedQuantity            AS OrderedQuantity
        , ts.RemainingQuantity_SalesUOM AS RemainingQuantity_SalesUOM
        , ts.RemainingQuantity          AS RemainingQuantity
        , ts.PriceUnit                  AS PriceUnit
        , ts.SalesUOM                   AS SalesUOM
        , ts.BaseUnitPrice_TransCur     AS BaseUnitPrice_TransCur
        , ts.TotalUnitPrice_TransCur    AS TotalUnitPrice_TransCur
        , ts.TotalAmount_TransCur       AS TotalAmount_TransCur
        , ts.CurrencyID                 AS CurrencyID
        , le.AccountingCurrencyID       AS AccountingCurrencyID
        , le.TransExchangeRateType      AS TransExchangeRateType
        , ts.LegalEntityID
        , ts.CustomerReference          AS CustomerReference
        , ts._SourceDate                AS _SourceDate
        , 1                             AS _SourceID
        , dql._RecID                    AS _RecID
      FROM salesquoteline_factstage     ts
    INNER JOIN {{ ref('salesquoteline_d') }} dql
        ON dql._RecID          = ts._RecID
      AND dql._SourceID       = 1
    INNER JOIN {{ ref('legalentity_d') }}    le
        ON le.LegalEntityID    = ts.LegalEntityID
      LEFT JOIN {{ ref('customer_d') }}       dc1
        ON dc1.LegalEntityID   = ts.LegalEntityID
      AND dc1.CustomerAccount = ts.CustomerAccount
      LEFT JOIN {{ ref('customer_d') }}       dc2
        ON dc2.LegalEntityID   = ts.LegalEntityID
      AND dc2.CustomerAccount = ts.InvoiceAccount
      LEFT JOIN {{ ref('date_d') }}           dd
        ON dd.Date             = ts.QuoteDate
      LEFT JOIN {{ ref('date_d') }}           dd1
        ON dd1.Date            = ts.ExpirationDate
      LEFT JOIN {{ ref('date_d') }}           dd2
        ON dd2.Date            = ts.ReceiptDateRequested
      LEFT JOIN {{ ref('date_d') }}           dd3
        ON dd3.Date            = ts.ShipDateRequested
      LEFT JOIN {{ ref('quotestatus_d') }}    qs
        ON qs.QuoteStatusID    = ts.QuoteStatusID
      LEFT JOIN {{ ref('quotetype_d') }}      qt
        ON qt.quotetypeid      = ts.QuoteTypeID
      LEFT JOIN {{ ref('financial_d') }}      fd
        ON fd._RecID           = ts.DefaultDimension
      AND fd._SourceID        = 1
      LEFT JOIN {{ ref('lot_d') }}            it
        ON it._recid           = ts.RecID_ITO
      AND it._sourceid        = 1
      LEFT JOIN {{ ref('inventorysite_d') }}  ds
        ON ds.LegalEntityID    = ts.LegalEntityID
      AND ds.InventorySiteID  = ts.InventSiteID
      LEFT JOIN {{ ref('product_d') }}        dp
        ON dp.LegalEntityID    = ts.LegalEntityID
      AND dp.ItemID           = ts.ItemID
      AND dp.ProductWidth     = ts.ProductWidth
      AND dp.ProductLength    = ts.ProductLength
      AND dp.ProductColor     = ts.ProductColor
      AND dp.ProductConfig    = ts.ProductConfig
      LEFT JOIN {{ ref('warehouse_d') }}      dw
        ON dw.LegalEntityID    = ts.LegalEntityID
      AND dw.WarehouseID      = ts.WarehouseID
      LEFT JOIN {{ ref('currency_d') }}       cc
        ON cc.CurrencyID       = ts.CurrencyID
      LEFT JOIN {{ ref('deliverymode_d') }}   ddm
        ON ddm.LegalEntityID   = ts.LegalEntityID
      AND ddm.DeliveryModeID  = ts.DeliveryModeID
      LEFT JOIN {{ ref('deliveryterm_d') }}   ddt
        ON ddt.LegalEntityID   = ts.LegalEntityID
      AND ddt.DeliveryTermID  = ts.DeliveryTermID
      LEFT JOIN {{ ref('paymentterm_d') }}    dpt
        ON dpt.LegalEntityID   = ts.LegalEntityID
      AND dpt.PaymentTermID   = ts.PaymentTermID
      LEFT JOIN {{ ref('paymentmode_d') }}    dpm
        ON dpm.LegalEntityID   = ts.LegalEntityID
      AND dpm.PaymentModeID   = ts.PaymentModeID
      LEFT JOIN {{ ref('employee_d') }}       de2
        ON de2._RecID          = ts.SalesTaker
      LEFT JOIN {{ ref('uom_d') }}            du
        ON du.UOM              = ts.InventoryUnit
      LEFT JOIN {{ ref('salescategory_d') }}  dsc
        ON dsc._RecID          = ts.RecID_SC
      AND dsc._SourceID       = 1
      LEFT JOIN {{ ref('uom_d') }}            du1
        ON du1.UOM             = ts.SalesUOM
      LEFT JOIN {{ ref('uom_d') }}            du2
        ON du2.UOM             = ts.QuotePriceUOM
      LEFT JOIN {{ ref('salesperson_d') }}    sp
        ON sp._RecID           = ts.SalesPersonID
      AND sp._SourceID        = 1;
)
SELECT DISTINCT td.SalesQuoteLineKey
      , td.LegalEntityKey
      , td.CurrencyKey
      , td.CustomerKey
      , td.FinancialKey
      , td.DeliveryModeKey
      , td.SalesPersonKey                                                                                            AS SalesPersonKey
      , td.DeliveryTermKey
      , td.ExpirationDateKey
      , td.InvoiceCustomerKey
      , td.LotKey
      , td.PaymentModeKey
      , td.PaymentTermKey
      , td.ProductKey
      , td.QuoteDateKey
      , td.QuoteStatusKey
      , td.QuoteTypeKey
      , td.ReceiptDateRequestedKey
      , td.SalesCategoryKey
      , td.SalesUOMKey
      , td.QuotePriceUOMKey
      , td.SalesTakerKey
      , td.ShipDateRequestedKey
      , td.InventorySiteKey
      , td.WarehouseKey
      , td.BaseUnitPrice_TransCur * ISNULL(ex.ExchangeRate, 1)                                                       AS BaseUnitPrice
      , td.BaseUnitPrice_TransCur
      , CASE WHEN td.DiscountAmount_TransCur * ISNULL(ex.ExchangeRate, 1) < 0
              THEN (td.DiscountAmount_TransCur * ISNULL(ex.ExchangeRate, 1)) * -1
              ELSE td.DiscountAmount_TransCur * ISNULL(ex.ExchangeRate, 1) END                                        AS DiscountAmount
      , CASE WHEN td.DiscountAmount_TransCur < 0 THEN td.DiscountAmount_TransCur * -1 ELSE
                                                                                      td.DiscountAmount_TransCur END AS DiscountAmount_TransCur
      , td.NetAmount_TransCur * ISNULL(ex.ExchangeRate, 1)                                                           AS NetAmount
      , td.NetAmount_TransCur
      , td.OrderedQuantity_SalesUOM
      , td.OrderedQuantity_SalesUOM * vuc.factor                                                                     AS OrderedQuantity_FT
      -- , td.OrderedQuantity_SalesUOM * vuc.factor * 12                                                                    AS OrderedQuantity_IN
      , td.OrderedQuantity_SalesUOM * vuc2.factor                                                                    AS OrderedQuantity_LB
      , ROUND(td.OrderedQuantity_SalesUOM * vuc3.factor, 0)                                                          AS OrderedQuantity_PC
      , td.OrderedQuantity_SalesUOM * vuc4.factor                                                                    AS OrderedQuantity_SQIN
      -- , td.OrderedQuantity_SalesUOM * vuc2.factor * 0.0005                                                                    AS OrderedQuantity_TON
      , td.OrderedQuantity
      , td.PriceUnit
      , td.RemainingQuantity_SalesUOM
      , td.RemainingQuantity_SalesUOM * vuc.factor                                                                   AS RemainingQuantity_FT
      --, td.RemainingQuantity_SalesUOM * vuc.factor * 12                                                                 AS RemainingQuantity_IN
      , td.RemainingQuantity_SalesUOM * vuc2.factor                                                                  AS RemainingQuantity_LB
      , ROUND(td.RemainingQuantity_SalesUOM * vuc3.factor, 0)                                                        AS RemainingQuantity_PC
      , td.RemainingQuantity_SalesUOM * vuc4.factor                                                                  AS RemainingQuantity_SQIN
      -- , td.RemainingQuantity_SalesUOM * vuc2.factor * 0.0005                                                                  AS RemainingQuantity_TON
      , td.RemainingQuantity
      , td.TotalAmount_TransCur * ISNULL(ex.ExchangeRate, 1)                                                         AS TotalAmount
      , td.TotalAmount_TransCur
      , td.TotalUnitPrice_TransCur * ISNULL(ex.ExchangeRate, 1)                                                      AS TotalUnitPrice
      , td.TotalUnitPrice_TransCur
      , td.CustomerReference                                                                                         AS CustomerReference
      , td._SourceDate                                                                                               AS _SourceDate
      , td._RecID
      , td._SourceID
      , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS  _CreatedDate
      , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
    FROM salesquoteline_factdetailmain                td
    LEFT JOIN {{ ref('vwuomconversion_ft') }}   vuc
      ON vuc.legalentitykey  = td.LegalEntityKey
    AND vuc.productkey      = td.ProductKey
    AND vuc.fromuomkey      = td.InventoryUOMKey
 -- AND vuc.touom           = 'FT'

    LEFT JOIN {{ ref('vwuomconversion_lb') }}   vuc2
      ON vuc2.legalentitykey = td.LegalEntityKey
    AND vuc2.productkey     = td.ProductKey
    AND vuc2.fromuomkey     = td.InventoryUOMKey
--  AND vuc2.touom          = 'LB'
    LEFT JOIN {{ ref('vwuomconversion_pc') }}   vuc3
      ON vuc3.legalentitykey = td.LegalEntityKey
    AND vuc3.productkey     = td.ProductKey
    AND vuc3.fromuomkey     = td.InventoryUOMKey
--  AND vuc3.touom          = 'PC'
    LEFT JOIN {{ ref('vwuomconversion_sqin') }}   vuc4
      ON vuc4.legalentitykey = td.LegalEntityKey
    AND vuc4.productkey     = td.ProductKey
    AND vuc4.fromuomkey     = td.InventoryUOMKey
--  AND vuc4.touom          = 'SQIN'
  --  LEFT JOIN {{ ref('vwuomconversion') }}   vuc5
  --   ON vuc5.legalentitykey = td.LegalEntityKey
  --  AND vuc5.productkey     = td.ProductKey
  --  AND vuc5.fromuomkey     = td.InventoryUOMKey
  --  AND vuc5.touom          = 'T'
    LEFT JOIN {{ ref('exchangerate_f') }} ex
      ON ex.ExchangeDateKey  = td.QuoteDateKey
    AND ex.FromCurrencyID   = td.CurrencyID
    AND ex.ToCurrencyID     = td.AccountingCurrencyID
    AND ex.ExchangeRateType = td.TransExchangeRateType;
