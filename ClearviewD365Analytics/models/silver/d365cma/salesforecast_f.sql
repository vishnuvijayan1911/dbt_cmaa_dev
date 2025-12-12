{{ config(materialized='table', tags=['silver'], alias='salesforecast_fact') }}

-- Source file: cma/cma/layers/_base/_silver/salesforecast_f/salesforecast_f.py
-- Root method: SalesforecastFact.salesforecast_factdetail [SalesForecast_FactDetail]
-- Inlined methods: SalesforecastFact.salesforecast_factstage [SalesForecast_FactStage], SalesforecastFact.salesforecast_factdetail1 [SalesForecast_FactDetail1]
-- external_table_name: SalesForecast_FactDetail
-- schema_name: temp

WITH
salesforecast_factstage AS (
    SELECT fs.dataareaid                             AS LEGALENTITYID
             , fm.modelid                                 AS MODELID
             , fs.itemid                                  AS ITEMID
             , id.inventsizeid                            AS INVENTSIZEID
             , id.inventcolorid                           AS INVENTCOLORID
             , id.inventstyleid                           AS INVENTSTYLEID
             , fs.defaultdimension                        AS DEFAULTDIMENSION
             , fs.custaccountid                           AS CUSTACCOUNTID
             , fs.salesunitid                             AS SALESUNITID
             , id.inventsiteid                            AS INVENTSITEID
             , id.inventlocationid                        AS INVENTLOCATIONID
             , id.configid                                AS PRODUCTCONFIG
             , ct.maincontactworker                       AS MAINCONTACTWORKER
             , fs.currency                                AS CURRENCY
             , fs.priceunit                               AS PRICEUNIT
             , fs.inventqty                               AS INVENTQTY
             , fs.salesqty                                AS SALESQTY
             , fs.amount                                  AS AMOUNT
             , fs.salesprice                              AS SALESPRICE
             , CAST(fs.startdate AS DATE)                 AS STARTDATE
             , CAST(fs.projforecastcostpaymdate AS DATE)  AS EXPECTEDCOSTPAYMENTDATE
             , CAST(fs.projforecastsalespaymdate AS DATE) AS EXPECTEDSALESPAYMENTDATE
             , CAST(fs.projforecastinvoicedate AS DATE)   AS EXPECTEDINVOICEDATE
             , fs.recid                                  AS _RecID
             , fm.recid                                  AS _RecID1

          FROM {{ ref('forecastsales') }}      fs
         INNER JOIN {{ ref('forecastmodel') }} fm
            ON fm.dataareaid = fs.dataareaid
           AND fm.modelid     = fs.modelid
           AND fm.type        = 0
         INNER JOIN {{ ref('inventdim') }}     id
            ON id.dataareaid = fs.dataareaid
           AND id.inventdimid = fs.inventdimid
          LEFT JOIN  {{ ref('custtable') }}     ct
            ON ct.dataareaid = fs.dataareaid
           AND ct.accountnum  = fs.custaccountid
         WHERE fs.active   = 1
          --AND fs.expandid <> 0;
),
salesforecast_factdetail1 AS (
    SELECT le.LegalEntityKey         AS LegalEntityKey
             , dc.CustomerKey            AS CustomerKey
             , cur.CurrencyKey           AS CurrencyKey
             , fd.FinancialKey           AS FinancialKey
             , dd.DateKey                AS ForecastStartDateKey
             , iv.InventorySiteKey       AS InventorySiteKey
             , ISNULL(dp.ProductKey, -1) AS ProductKey
             , dm.ForecastModelKey  AS SalesForecastModelKey
             , dsp.SalesPersonKey        AS SalesPersonKey
             , du.UOMKey                 AS SalesUOMKey
             , dd2.DateKey               AS ExpectedCostPaymentDateKey
             , dd3.DateKey               AS ExpectedInvoiceDateKey
             , dd4.DateKey               AS ExpectedSalesPaymentDateKey
             , dw.WarehouseKey           AS WarehouseKey
             , ts.INVENTQTY              AS ForecastQuantity
             , ts.SALESQTY               AS ForecastQuantity_SalesUOM
             , ts.AMOUNT                 AS ForecastAmount
             , ts.PRICEUNIT              AS PriceUnit
             , ts.SALESPRICE             AS SalesPrice
             , ts._RecID                 AS _RecID
             , 1                         AS _SourceID

          FROM salesforecast_factstage                      ts
         INNER JOIN {{ ref('legalentity_d') }}        le
            ON le.LegalEntityID   = ts.LEGALENTITYID
          LEFT JOIN {{ ref('financial_d') }}          fd
            ON fd._RecID          = ts.DEFAULTDIMENSION
           AND fd._SourceID       = 1
          LEFT JOIN {{ ref('forecastmodel_d') }} dm
            ON dm.LegalEntityID   = ts.LEGALENTITYID
           AND dm.ModelID         = ts.MODELID
          LEFT JOIN {{ ref('product_d') }}            dp
            ON dp.LegalEntityID   = ts.LEGALENTITYID
           AND dp.ItemID          = ts.ITEMID
           AND dp.ProductWidth    = ts.INVENTSIZEID
           AND dp.ProductLength   = ts.INVENTCOLORID
           AND dp.ProductColor    = ts.INVENTSTYLEID
           AND dp.ProductConfig   = ts.PRODUCTCONFIG
          LEFT JOIN {{ ref('customer_d') }}           dc
            ON dc.LegalEntityID   = ts.LEGALENTITYID
           AND dc.CustomerAccount = ts.CUSTACCOUNTID
          LEFT JOIN {{ ref('inventorysite_d') }}      iv
            ON iv.LegalEntityID   = ts.LEGALENTITYID
           AND iv.InventorySiteID = ts.INVENTSITEID
          LEFT JOIN {{ ref('warehouse_d') }}          dw
            ON dw.LegalEntityID   = ts.LEGALENTITYID
           AND dw.WarehouseID     = ts.INVENTLOCATIONID
          LEFT JOIN {{ ref('date_d') }}               dd
            ON dd.Date            = ts.STARTDATE
          LEFT JOIN {{ ref('date_d') }}               dd2
            ON dd2.Date           = ts.EXPECTEDCOSTPAYMENTDATE
          LEFT JOIN {{ ref('date_d') }}               dd3
            ON dd3.Date           = ts.EXPECTEDINVOICEDATE
          LEFT JOIN {{ ref('date_d') }}               dd4
            ON dd4.Date           = ts.EXPECTEDSALESPAYMENTDATE
          LEFT JOIN {{ ref('currency_d') }}           cur
            ON cur.CurrencyID     = ts.CURRENCY
          LEFT JOIN {{ ref('uom_d') }}                du
            ON du.UOM             = ts.SALESUNITID
          LEFT JOIN {{ ref('salesperson_d') }}        dsp
            ON dsp._RecID         = ts.MAINCONTACTWORKER
           AND dsp._SourceID      = 1;
)
SELECT 
         , {{ dbt_utils.generate_surrogate_key(['ts._RecID', 'ts._SourceID']) }} AS SalesForecastKey
         , ts.LegalEntityKey
         , ts.CustomerKey
         , ts.CurrencyKey                              AS CurrencyKey
         , ts.FinancialKey
         , ts.ForecastStartDateKey                     AS ForecastDateKey
         , ts.InventorySiteKey
         , ISNULL(ts.SalesForecastModelKey, -1)        AS SalesForecastModelKey
         , ts.ProductKey
         , ts.SalesPersonKey
         , ts.SalesUOMKey
         , ts.ExpectedCostPaymentDateKey
         , ts.ExpectedInvoiceDateKey
         , ts.ExpectedSalesPaymentDateKey
         , ts.WarehouseKey
         , ts.ForecastQuantity
         , ts.ForecastQuantity * vuc.factor            AS ForecastQuantity_FT
         , ts.ForecastQuantity * vuc2.factor           AS ForecastQuantity_LB
         , ROUND(ts.ForecastQuantity * vuc3.factor, 0) AS ForecastQuantity_PC
         , ts.ForecastQuantity * vuc4.factor           AS ForecastQuantity_SQIN
         , ts.ForecastQuantity_SalesUOM
         , ts.ForecastQuantity_SalesUOM * vuc.factor            AS ForecastQuantity_SalesUOM_FT
         , ts.ForecastQuantity_SalesUOM * vuc2.factor           AS ForecastQuantity_SalesUOM_LB
         , ROUND(ts.ForecastQuantity_SalesUOM * vuc3.factor, 0) AS ForecastQuantity_SalesUOM_PC
         , ts.ForecastQuantity_SalesUOM * vuc4.factor           AS ForecastQuantity_SalesUOM_SQIN
         , ts.ForecastAmount
         , ts.PriceUnit
         , ts.SalesPrice
         , ts._RecID
         , ts._SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM salesforecast_factdetail1                 ts
      LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc
        ON vuc.legalentitykey  = ts.LegalEntityKey
       AND vuc.productkey      = ts.ProductKey
       AND vuc.fromuomkey      = ts.SalesUOMKey
    -- AND vuc.touom           = 'FT'





      LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc2
        ON vuc2.legalentitykey = ts.LegalEntityKey
       AND vuc2.productkey     = ts.ProductKey
       AND vuc2.fromuomkey     = ts.SalesUOMKey
    -- AND vuc2.touom          = 'LB'
      LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc3
        ON vuc3.legalentitykey = ts.LegalEntityKey
       AND vuc3.productkey     = ts.ProductKey
       AND vuc3.fromuomkey     = ts.SalesUOMKey
    -- AND vuc3.touom          = 'PC'
      LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc4
        ON vuc4.legalentitykey = ts.LegalEntityKey
       AND vuc4.productkey     = ts.ProductKey
       AND vuc4.fromuomkey     = ts.SalesUOMKey
    -- AND vuc4.touom          = 'SQIN'
