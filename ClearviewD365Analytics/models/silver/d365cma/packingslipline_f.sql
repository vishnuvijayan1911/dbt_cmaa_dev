{{ config(materialized='table', tags=['silver'], alias='packingslipline_fact') }}

-- Source file: cma/cma/layers/_base/_silver/packingslipline_f/packingslipline_f.py
-- Root method: PackingsliplineFact.packingslipline_factdetail [PackingSlipLine_FactDetail]
-- Inlined methods: PackingsliplineFact.packingslipline_factstagewhsloadtablecustpackingslipJour [PackingSlipLine_FactWHSLoadTableCustPackingSlipJour], PackingsliplineFact.packingslipline_factshippednotinvoiced [PackingSlipLine_FactShippedNotInvoiced], PackingsliplineFact.packingslipline_factstage [PackingSlipLine_FactStage], PackingsliplineFact.packingslipline_factline [PackingSlipLine_FactLine]
-- external_table_name: PackingSlipLine_FactDetail
-- schema_name: temp

WITH
packingslipline_factwhsloadtablecustpackingslipjour AS (
    SELECT wspj.whsloadtabledataareaid,wspj.custpackingslipjourrecid,MIN(wspj.whsloadtableloadid) AS WHSLOADTABLELOADID
    				FROM {{ ref('whsloadtablecustpackingslipjour') }} wspj
    				GROUP BY wspj.whsloadtabledataareaid
    					,wspj.custpackingslipjourrecid
),
packingslipline_factshippednotinvoiced AS (
    SELECT SUM(it.qty) * -1                                                                      AS ShippedNotInvoicedQuantity
             , MAX(CASE WHEN it.statusissue IN ( 2 ) OR it.statusreceipt IN ( 2 ) THEN 1 ELSE 0 END) AS ShippedNotInvoicedLineCount
             , MAX(cpst.recid)                                                                      AS RecID_CPST

          FROM {{ ref('custpackingsliptrans') }} cpst
         INNER JOIN {{ ref('inventtrans') }}     it
            ON it.dataareaid   = cpst.dataareaid
           AND it.packingslipid = cpst.packingslipid
           AND it.itemid        = cpst.itemid
         WHERE it.statusissue IN ( 2 )
            OR it.statusreceipt IN ( 2 )
         GROUP BY cpst.recid;
),
packingslipline_factstage AS (
    SELECT cpst.currencycode                                                                   AS CurrencyID
             , st.custaccount                                                                      AS CustAccount
             , cpst.dataareaid                                                                     AS LegalEntityID
             , cpst.defaultdimension                                                               AS DEFAULTDIMENSION
             , cpst.deliverydate                                                                   AS DeliveryDate
             , wspj.whsloadtableloadid                                                             AS LOADID
             , id.inventsiteid                                                                     AS SiteID
             , st.invoiceaccount                                                                   AS InvoiceAccount
             , id.inventlocationid                                                                 AS WarehouseID
             , ito.recid                                                                           AS RecID_ITO
             , id.inventsizeid                                                                     AS ProductWidth
             , id.inventcolorid                                                                    AS ProductLength
             , id.inventstyleid                                                                    AS ProductColor
             , id.configid                                                                         AS ProductConfig
             , cpsj.dlvmode                                                                        AS DeliveryModeID
             , cpst.dlvterm                                                                        AS DeliveryTermID
             , cpsj.salestype                                                                      AS SalesTypeID
             , cpst.cmapriceuom                                                             AS PricingUnit
             , cpst.salesunit                                                                      AS SalesUOM
             , cpsj.ledgervoucher                                                                  AS VoucherID
             , cpst.itemid                                                                         AS ItemID
             , cpst.ordered                                                                        AS OrderedQuantity_SalesUOM
             , cpst.ordered * cpst.inventqty / ISNULL(NULLIF(cpst.qty, 0), 1)                      AS OrderedQuantity
             , cpst.remain                                                                         AS RemainingQuantity_SalesUOM
             , cpst.remaininvent                                                                   AS RemainingQuantity
             , cpst.qty                                                                            AS ShippedQuantity_SalesUOM
             , cpst.inventqty                                                                      AS ShippedQuantity
             , cpst.valuemst                                                                       AS ShippedAmount
             , cpst.valuemst                                                                       AS ShippedAmount_TransCur
             , sni.ShippedNotInvoicedQuantity                                                      AS ShippedNotInvoicedQuantity
             , sni.ShippedNotInvoicedQuantity * sl.salesprice / ISNULL(NULLIF(sl.priceunit, 0), 1) AS ShippedNotInvoicedAmount_TransCur
             , sni.ShippedNotInvoicedLineCount
             , CASE WHEN (sl.confirmeddlv IS NULL OR CAST(sl.confirmeddlv AS DATE) <= '1/1/1900')
                    THEN 5
                    WHEN CAST(COALESCE(NULLIF(sl.confirmeddlv, '1/1/1900'), sl.shippingdaterequested) AS DATE) >= CAST(cpst.deliverydate AS DATE)
                    THEN 4
                    WHEN CAST(COALESCE(NULLIF(sl.confirmeddlv, '1/1/1900'), sl.shippingdaterequested) AS DATE) < CAST(cpst.deliverydate AS DATE)
                    THEN 3
                    ELSE NULL END                                                                  AS OnTimeShipStatusID
             , cpst.priceunit                                                                      AS PriceUnit
             , sl.recid                                                                           AS RecID_SL
             , cpst.recid                                                                         AS _RecID
             , 1                                                                                   AS _SourceID
          FROM {{ ref('custpackingsliptrans') }}                 cpst
         INNER JOIN {{ ref('custpackingslipjour') }}             cpsj
            ON cpsj.dataareaid              = cpst.dataareaid
           AND cpsj.salesid                  = cpst.salesid
           AND cpsj.packingslipid            = cpst.packingslipid
           AND cpsj.deliverydate             = cpst.deliverydate
          LEFT JOIN {{ ref('salestable') }}                    st
            ON st.dataareaid                = cpst.dataareaid
           AND st.salesid                    = cpst.salesid
          LEFT JOIN {{ ref('salesline') }}                       sl
            ON sl.dataareaid                = cpst.dataareaid
           AND sl.inventtransid              = cpst.inventtransid
           AND sl.itemid                     = cpst.itemid
         INNER JOIN {{ ref('inventdim') }}                       id
            ON id.dataareaid                = cpst.dataareaid
           AND id.inventdimid                = cpst.inventdimid
          LEFT JOIN {{ ref('inventtransorigin') }}               ito
            ON ito.dataareaid               = cpst.dataareaid
           AND ito.inventtransid             = cpst.inventtransid
          LEFT JOIN packingslipline_factwhsloadtablecustpackingslipjour wspj
            ON wspj.whsloadtabledataareaid   = cpsj.dataareaid
           AND wspj.custpackingslipjourrecid = cpsj.recid
          LEFT JOIN packingslipline_factshippednotinvoiced                 sni
            ON sni.RecID_CPST                = cpst.recid
),
packingslipline_factline AS (
    SELECT dcu.CustomerKey                                                   AS CustomerKey
             , cur.CurrencyKey                                                   AS CurrencyKey
             , fc.AddressKey                                                     AS DeliveryAddressKey
             , dm.DeliveryModeKey                                                AS DeliveryModeKey
             , tm.DeliveryTermKey                                                AS DeliveryTermKey
             , fd.FinancialKey                                                   AS FinancialKey
             , ds.InventorySiteKey                                               AS InventorySiteKey
             , dcu1.CustomerKey                                                  AS InvoiceCustomerKey
             , le.LegalEntityKey                                                 AS LegalEntityKey
             , dl.LotKey                                                         AS LotKey
             , ot.OnTimeShipStatusKey                                            AS OnTimeShipStatusKey
             , dd.DateKey                                                        AS PackingSlipDateKey
             , dcpl.PackingSlipLineKey                                           AS PackingSlipLineKey
             , pu.UOMKey                                                         AS PricingUOMKey
             , ISNULL(dp.ProductKey, -1)                                         AS ProductKey
             , sol.SalesOrderLineKey                                             AS SalesOrderLineKey
             , pt.SalesTypeKey                                                   AS SalesTypeKey
             , su.UOMKey                                                         AS SalesUOMKey
             , dsl.ShippingLoadKey                                               AS ShippingLoadKey
             , vou.VoucherKey                                                    AS VoucherKey
             , dw.WarehouseKey                                                   AS WarehouseKey
             , ts.OrderedQuantity_SalesUOM                                       AS OrderedQuantity_SalesUOM
             , ts.OrderedQuantity                                                AS OrderedQuantity
             , ts.RemainingQuantity_SalesUOM                                     AS RemainingQuantity_SalesUOM
             , ts.RemainingQuantity                                              AS RemainingQuantity
             , ts.SalesUOM                                                       AS SalesUOM
             , ts.ShippedQuantity_SalesUOM                                       AS ShippedQuantity_SalesUOM
             , ts.ShippedQuantity                                                AS ShippedQuantity
             , ts.ShippedNotInvoicedQuantity                                     AS ShippedNotInvoicedQuantity
             , ts.ShippedNotInvoicedQuantity * vuc1.factor                       AS ShippedNotInvoicedQuantity_SalesUOM
             , ts.ShippedNotInvoicedAmount_TransCur * ISNULL(ex.ExchangeRate, 1) AS ShippedNotInvoicedAmount
             , ts.ShippedNotInvoicedAmount_TransCur                              AS ShippedNotInvoicedAmount_TransCur
             , ts.ShippedNotInvoicedLineCount
             , ts.ShippedAmount                                                  AS ShippedAmount
             , ts.ShippedAmount * ISNULL(ex.ExchangeRate, 1)                     AS ShippedAmount_TransCur
             , le.AccountingCurrencyID                                           AS AccountingCurrencyID
             , dp.InventoryUOM                                                   AS InventoryUOM
             , ts.PriceUnit                                                      AS PriceUnit
             , le.TransExchangeRateType                                          AS TransExchangeRateType
             , ts._RecID                                                         AS _RecID
             , 1                                                                 AS _SourceID

          FROM packingslipline_factstage                     ts
         INNER JOIN silver.cma_LegalEntity       le
            ON le.LegalEntityID      = ts.LegalEntityID
         INNER JOIN silver.cma_PackingSlipLine   dcpl
            ON dcpl._RecID           = ts._RecID
           AND dcpl._SourceID        = 1
          LEFT JOIN silver.cma_Product           dp
            ON dp.LegalEntityID      = ts.LegalEntityID
           AND dp.ItemID             = ts.ItemID
           AND dp.ProductWidth       = ts.ProductWidth
           AND dp.ProductLength      = ts.ProductLength
           AND dp.ProductColor       = ts.ProductColor
           AND dp.ProductConfig      = ts.ProductConfig
          LEFT JOIN silver.cma_SalesOrderLine    sol
            ON sol._RecID            = ts.RecID_SL
           AND sol._SourceID         = 1
          LEFT JOIN silver.cma_Financial         fd
            ON fd._RecID             = ts.DEFAULTDIMENSION
           AND fd._SourceID          = 1
          LEFT JOIN silver.cma_Customer          dcu
            ON dcu.LegalEntityID     = ts.LegalEntityID
           AND dcu.CustomerAccount   = ts.CustAccount
          LEFT JOIN silver.cma_Customer          dcu1
            ON dcu1.LegalEntityID    = ts.LegalEntityID
           AND dcu1.CustomerAccount  = ts.InvoiceAccount
          LEFT JOIN silver.cma_Customer_Fact     fc
            ON fc.CustomerKey        = dcu.CustomerKey
          LEFT JOIN silver.cma_InventorySite     ds
            ON ds.LegalEntityID      = ts.LegalEntityID
           AND ds.InventorySiteID    = ts.SiteID
          LEFT JOIN silver.cma_ShippingLoad      dsl
            ON dsl.LegalEntityID     = ts.LegalEntityID
           AND dsl.LoadID            = ts.LoadID
          LEFT JOIN silver.cma_Date              dd
            ON dd.Date               = ts.DeliveryDate
          LEFT JOIN silver.cma_Warehouse         dw
            ON dw.LegalEntityID      = ts.LegalEntityID
           AND dw.WarehouseID        = ts.WarehouseID
          LEFT JOIN silver.cma_Lot               dl
            ON dl._RecID             = ts.RecID_ITO
           AND dl._SourceID          = 1
          LEFT JOIN silver.cma_DeliveryMode      dm
            ON dm.LegalEntityID      = ts.LegalEntityID
           AND dm.DeliveryModeID     = ts.DeliveryModeID
          LEFT JOIN silver.cma_DeliveryTerm      tm
            ON tm.LegalEntityID      = ts.LegalEntityID
           AND tm.DeliveryTermID     = ts.DeliveryTermID
          LEFT JOIN silver.cma_UOM               su
            ON su.UOM                = ts.SalesUOM
          LEFT JOIN silver.cma_UOM               pu
            ON pu.UOM                = ts.PricingUnit
          LEFT JOIN silver.cma_SalesType         pt
            ON pt.SalesTypeID        = ts.SalesTypeID
          LEFT JOIN silver.cma_Currency          cur
            ON cur.CurrencyID        = ts.CurrencyID
          LEFT JOIN silver.cma_Voucher           vou
            ON vou.LegalEntityID     = ts.LegalEntityID
           AND vou.VoucherID         = ts.VoucherID
          LEFT JOIN silver.cma_OnTimeShipStatus  ot
            ON ot.OnTimeShipStatusID = ts.OnTimeShipStatusID
          LEFT JOIN silver.cma_ExchangeRate_Fact ex
            ON ex.ExchangeDateKey    = dd.DateKey
           AND ex.FromCurrencyID     = le.AccountingCurrencyID
           AND ex.ToCurrencyID       = ts.CurrencyID
           AND ex.ExchangeRateType   = le.TransExchangeRateType
          LEFT JOIN {{ ref('vwuomconversion') }}   vuc1
            ON vuc1.legalentityid    = ts.LegalEntityID
           AND vuc1.productkey       = dp.ProductKey
           AND vuc1.fromuom          = dp.InventoryUOM
           AND vuc1.touom            = ts.SalesUOM;
)
SELECT DISTINCT tl.PackingSlipLineKey
         , tl.CurrencyKey
         , tl.CustomerKey
         , tl.DeliveryAddressKey
         , tl.DeliveryModeKey
         , tl.DeliveryTermKey
         , tl.FinancialKey
         , tl.InvoiceCustomerKey
         , tl.LegalEntityKey
         , tl.LotKey
         , tl.OnTimeShipStatusKey
         , tl.PackingSlipDateKey
         , tl.PricingUOMKey
         , tl.ProductKey
         , tl.SalesOrderLineKey
         , tl.SalesTypeKey
         , tl.SalesUOMKey
         , tl.ShippingLoadKey
         , tl.InventorySiteKey
         , tl.VoucherKey
         , tl.WarehouseKey
         , tl.OrderedQuantity_SalesUOM
         , tl.OrderedQuantity_SalesUOM * ISNULL(vuc.factor, 0)                       AS OrderedQuantity_FT

         , tl.OrderedQuantity_SalesUOM * ISNULL(vuc2.factor, 0)                      AS OrderedQuantity_LB
         , ROUND(tl.OrderedQuantity_SalesUOM * ISNULL(vuc3.factor, 0), 0)            AS OrderedQuantity_PC
         , tl.OrderedQuantity_SalesUOM * ISNULL(vuc4.factor, 0)                      AS OrderedQuantity_SQIN

         , tl.OrderedQuantity
         , tl.PriceUnit
         , tl.ShippedQuantity_SalesUOM
         , tl.ShippedQuantity_SalesUOM * ISNULL(vuc.factor, 0)                       AS ShippedQuantity_FT

         , tl.ShippedQuantity_SalesUOM * ISNULL(vuc2.factor, 0)                      AS ShippedQuantity_LB
         , ROUND(tl.ShippedQuantity_SalesUOM * ISNULL(vuc3.factor, 0), 0)            AS ShippedQuantity_PC
         , tl.ShippedQuantity_SalesUOM * ISNULL(vuc4.factor, 0)                      AS ShippedQuantity_SQIN

         , tl.ShippedQuantity
         , tl.ShippedNotInvoicedQuantity_SalesUOM									 AS ShippedNotInvoicedQuantity_SalesUOM
         , tl.ShippedNotInvoicedQuantity_SalesUOM * ISNULL(vuc.factor, 0)            AS ShippedNotInvoicedQuantity_FT

         , tl.ShippedNotInvoicedQuantity_SalesUOM * ISNULL(vuc2.factor, 0)           AS ShippedNotInvoicedQuantity_LB
         , ROUND(tl.ShippedNotInvoicedQuantity_SalesUOM * ISNULL(vuc3.factor, 0), 0) AS ShippedNotInvoicedQuantity_PC
         , tl.ShippedNotInvoicedQuantity_SalesUOM * ISNULL(vuc4.factor, 0)           AS ShippedNotInvoicedQuantity_SQIN

         , tl.ShippedNotInvoicedQuantity											 AS ShippedNotInvoicedQuantity
         , tl.ShippedNotInvoicedAmount
         , tl.ShippedNotInvoicedAmount_TransCur
         , tl.ShippedNotInvoicedLineCount
         , tl.RemainingQuantity_SalesUOM
         , tl.RemainingQuantity_SalesUOM * ISNULL(vuc.factor, 0)                     AS RemainingQuantity_FT

         , tl.RemainingQuantity_SalesUOM * ISNULL(vuc2.factor, 0)                    AS RemainingQuantity_LB
         , ROUND(tl.RemainingQuantity_SalesUOM * ISNULL(vuc3.factor, 0), 0)          AS RemainingQuantity_PC
         , tl.RemainingQuantity_SalesUOM * ISNULL(vuc4.factor, 0)                    AS RemainingQuantity_SQIN

         , tl.RemainingQuantity
         , tl.ShippedAmount
         , tl.ShippedAmount_TransCur
         , tl._SourceID
         , tl._RecID
         , CURRENT_TIMESTAMP                                                            AS _CreatedDate
         , CURRENT_TIMESTAMP                                                            AS _ModifiedDate  

      FROM  packingslipline_factline                   tl
      LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc
        ON vuc.legalentitykey  = tl.LegalEntityKey
       AND vuc.productkey      = tl.ProductKey
       AND vuc.fromuomkey      = tl.SalesUOMKey
    -- AND vuc.touom           = 'FT'





      LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc2
        ON vuc2.legalentitykey = tl.LegalEntityKey
       AND vuc2.productkey     = tl.ProductKey
       AND vuc2.fromuomkey     = tl.SalesUOMKey
    -- AND vuc2.touom          = 'LB'
      LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc3
        ON vuc3.legalentitykey = tl.LegalEntityKey
       AND vuc3.productkey     = tl.ProductKey
       AND vuc3.fromuomkey     = tl.SalesUOMKey
    -- AND vuc3.touom          = 'PC'
      LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc4
        ON vuc4.legalentitykey = tl.LegalEntityKey
       AND vuc4.productkey     = tl.ProductKey
       AND vuc4.fromuomkey     = tl.SalesUOMKey
    -- AND vuc4.touom          = 'SQIN'
