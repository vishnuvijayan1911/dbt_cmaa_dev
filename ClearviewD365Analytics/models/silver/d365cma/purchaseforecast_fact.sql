{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/purchaseforecast_fact/purchaseforecast_fact.py
-- Root method: PurchaseforecastFact.purchaseforecast_factdetail [PurchaseForecast_FactDetail]
-- Inlined methods: PurchaseforecastFact.purchaseforecast_factstage [PurchaseForecast_FactStage], PurchaseforecastFact.purchaseforecast_factdetail1 [PurchaseForecast_FactDetail1]
-- external_table_name: PurchaseForecast_FactDetail
-- schema_name: temp

WITH
purchaseforecast_factstage AS (
    SELECT ps.dataareaid             AS LEGALENTITYID
             , fm.modelid                 AS MODELID
             , ps.itemid                  AS ITEMID
             , id.inventsizeid            AS INVENTSIZEID
             , id.inventcolorid           AS INVENTCOLORID
             , id.inventstyleid           AS INVENTSTYLEID
             , ps.defaultdimension        AS DEFAULTDIMENSION
             , ps.vendaccountid           AS VENDACCOUNTID
             , ps.purchunitid             AS PURCHUNITID
             , id.inventsiteid            AS INVENTSITEID
             , id.inventlocationid        AS INVENTLOCATIONID
             , id.configid                AS PRODUCTCONFIG
             , ps.currency                AS CURRENCY
             , ps.priceunit               AS PRICEUNIT
             , ps.inventqty               AS INVENTQTY
             , ps.purchqty                AS PURCHQTY
             , ps.amount                  AS AMOUNT
             , ps.purchprice              AS PURCHPRICE
             , CAST(ps.startdate AS DATE) AS STARTDATE
         , ps.recid                  AS _RecID
             , fm.recid                  AS _RecID1

          FROM {{ ref('forecastpurch') }}      ps

         INNER JOIN {{ ref('forecastmodel') }} fm
            ON fm.dataareaid = ps.dataareaid
           AND fm.modelid     = ps.modelid
           AND fm.type        = 0
         INNER JOIN {{ ref('inventdim') }}     id
            ON id.dataareaid = ps.dataareaid
           AND id.inventdimid = ps.inventdimid
         WHERE ps.expandid <> 0
           AND ps.active   = 1;
),
purchaseforecast_factdetail1 AS (
    SELECT le.LegalEntityKey          AS LegalEntityKey
             , cur.CurrencyKey            AS CurrencyKey
             , fd.FinancialKey            AS FinancialKey
             , dd.DateKey                 AS ForecastStartDateKey
             , iv.InventorySiteKey        AS InventorySiteKey
             , ISNULL (dp.ProductKey, -1) AS ProductKey
             , dm.ForecastModelKey        AS ForecastModelKey
             , du.UOMKey                  AS PurchUOMKey
             , dv.VendorKey               AS VendorKey
             , dw.WarehouseKey            AS WarehouseKey
             , ts.INVENTQTY               AS ForecastQuantity
             , ts.PURCHQTY                AS ForecastQuantity_PURCHUOM
             , ts.AMOUNT                  AS ForecastAmount
             , ts.PRICEUNIT               AS PriceUnit
             , ts.PURCHPRICE              AS PurchasePrice
             , ts._RecID                  AS _RecID
             , 1                          AS _SourceID

        FROM purchaseforecast_factstage                ts
         INNER JOIN silver.cma_LegalEntity   le
            ON le.LegalEntityID   = ts.LEGALENTITYID
          LEFT JOIN silver.cma_Financial     fd
            ON fd._RecID          = ts.DEFAULTDIMENSION
           AND fd._SourceID       = 1
          LEFT JOIN silver.cma_ForecastModel dm
            ON dm.LegalEntityID   = ts.LEGALENTITYID
           AND dm.ModelID         = ts.MODELID
          LEFT JOIN silver.cma_Product       dp
            ON dp.LegalEntityID   = ts.LEGALENTITYID
           AND dp.ItemID          = ts.ItemID
           AND dp.ProductWidth    = ts.INVENTSIZEID
           AND dp.ProductLength   = ts.INVENTCOLORID
           AND dp.ProductColor    = ts.INVENTSTYLEID
           AND dp.ProductConfig   = ts.PRODUCTCONFIG
          LEFT JOIN silver.cma_Vendor        dv
            ON dv.LegalEntityID   = ts.LEGALENTITYID
           AND dv.VendorAccount   = ts.VENDACCOUNTID
          LEFT JOIN silver.cma_InventorySite iv
            ON iv.LegalEntityID   = ts.LEGALENTITYID
           AND iv.InventorySiteID = ts.INVENTSITEID
          LEFT JOIN silver.cma_Warehouse     dw
            ON dw.LegalEntityID   = ts.LEGALENTITYID
           AND dw.WarehouseID     = ts.INVENTLOCATIONID
          LEFT JOIN silver.cma_Date          dd
            ON dd.Date            = ts.STARTDATE
          LEFT JOIN silver.cma_Currency      cur
            ON cur.CurrencyID     = ts.Currency
          LEFT JOIN silver.cma_UOM           du
            ON du.UOM             = ts.PURCHUNITID;
)
SELECT 
           CURRENT_TIMESTAMP                                                                      AS _CreatedDate
         , CURRENT_TIMESTAMP                                                                      AS _ModifiedDate 
         , ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS PurchaseForecastKey,
    ts.LegalEntityKey
         , ts.CurrencyKey										   AS CurrencyKey
         , ts.FinancialKey
         , ts.ForecastStartDateKey								   AS ForecastDateKey
         , ts.InventorySiteKey
         , ISNULL (ts.ForecastModelKey, -1)						   AS ForecastModelKey
         , ts.ProductKey
         , ts.PurchUOMKey
         , ts.VendorKey
         , ts.WarehouseKey
         , ts.ForecastQuantity
         , ts.ForecastQuantity * ISNULL(vuc.factor, 0)             AS ForecastQuantity_FT

         , ts.ForecastQuantity * ISNULL(vuc2.factor, 0)            AS ForecastQuantity_LB
         , ROUND (ts.ForecastQuantity * ISNULL(vuc3.factor, 0), 0) AS ForecastQuantity_PC
         , ts.ForecastQuantity * ISNULL(vuc4.factor, 0)            AS ForecastQuantity_SQIN

         , ts.ForecastQuantity_PurchUOM
         , ts.ForecastAmount
         , ts.PriceUnit
         , ts.PurchasePrice
         , ts._RecID
         , ts._SourceID

      FROM purchaseforecast_factdetail1               ts
      LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc
        ON vuc.legalentitykey  = ts.LegalEntityKey
       AND vuc.productkey      = ts.ProductKey
       AND vuc.fromuomkey      = ts.PurchUOMKey
    -- AND vuc.touom           = 'FT'





      LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc2
        ON vuc2.legalentitykey = ts.LegalEntityKey
       AND vuc2.productkey     = ts.ProductKey
       AND vuc2.fromuomkey     = ts.PurchUOMKey
    -- AND vuc2.touom          = 'LB'
      LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc3
        ON vuc3.legalentitykey = ts.LegalEntityKey
       AND vuc3.productkey     = ts.ProductKey
       AND vuc3.fromuomkey     = ts.PurchUOMKey
    -- AND vuc3.touom          = 'PC'
      LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc4
        ON vuc4.legalentitykey = ts.LegalEntityKey
       AND vuc4.productkey     = ts.ProductKey
       AND vuc4.fromuomkey     = ts.PurchUOMKey
    -- AND vuc4.touom          = 'SQIN'
