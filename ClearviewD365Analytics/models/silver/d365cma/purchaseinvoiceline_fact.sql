{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/purchaseinvoiceline_fact/purchaseinvoiceline_fact.py
-- Root method: PurchaseinvoicelineFact.purchaseinvoiceline_factdetail [PurchaseInvoiceLine_FactDetail]
-- Inlined methods: PurchaseinvoicelineFact.purchaseinvoiceline_factstage [PurchaseInvoiceLine_FactStage], PurchaseinvoicelineFact.purchaseinvoiceline_factcharge [PurchaseInvoiceLine_FactCharge], PurchaseinvoicelineFact.purchaseinvoiceline_factline [PurchaseInvoiceLine_FactLine]
-- external_table_name: PurchaseInvoiceLine_FactDetail
-- schema_name: temp

WITH
purchaseinvoiceline_factstage AS (
    SELECT vij.dataareaid                                                                                             AS LegalEntityID
             , vij.invoicedate                                                                                             AS InvoiceDate
             , vij.orderaccount                                                                                            AS OrderAccount
             , vij.invoiceaccount                                                                                          AS InvoiceAccount
             , vij.ledgervoucher                                                                                           AS VoucherID
             , vij.cashdisccode                                                                                            AS CashDiscountID
             , vij.purchasetype                                                                                            AS PurchaseTypeID
             , vij.currencycode                                                                                            AS CurrencyID
             , ph.dlvterm                                                                                                  AS DeliveryTermID
             , ph.dlvmode                                                                                                  AS DeliveryModeID
             , ph.payment                                                                                                  AS PaymentTermID
             , pl.cmapriceuom                                                                                      AS PricingUnit
             , ph.taxgroup                                                                                                 AS TaxGroupID
             , ito.recid                                                                                                  AS RecID_ITO
             , ISNULL (vit.defaultdimension, vij.defaultdimension)                                                         AS DEFAULTDIMENSION
             , vit.itemid                                                                                                  AS ItemID
             , vit.deliverypostaladdress                                                                                   AS DELIVERYPOSTALADDRESS
             , pl.recid                                                                                                   AS RecID_PL
             , id.inventcolorid                                                                                            AS ProductLength
             , id.inventstyleid                                                                                            AS ProductColor
             , id.inventsizeid                                                                                             AS ProductWidth
             , id.configid                                                                                                 AS ProductConfig
             , id.inventsiteid                                                                                             AS SiteID
             , id.inventlocationid                                                                                         AS WarehouseID
             , vit.purchunit                                                                                       AS PurchaseUnit
             , CASE WHEN ISNULL (vit.qty, 0) = 0
                    THEN vit.lineamount
                    ELSE (vit.purchprice * vit.qty / ISNULL (NULLIF(vit.priceunit, 0), 1)) END * vij.exchrate / 100        AS BaseAmount
             , CASE WHEN ISNULL (vit.qty, 0) = 0
                    THEN vit.lineamount
                    ELSE (vit.purchprice * vit.qty / ISNULL (NULLIF(vit.priceunit, 0), 1)) END                             AS BaseAmount_TransCur
             , CASE WHEN vit.lineamount IS NULL THEN vij.invoiceamountmst ELSE vit.lineamount * vij.exchrate / 100 END     AS NetAmount
             , ISNULL (vit.lineamount, vij.invoiceamount)                                                                  AS NetAmount_TransCur
             , CASE WHEN vit.cmanetamount IS NULL THEN vij.invoiceamountmst ELSE vit.cmanetamount * vij.exchrate / 100 END AS InvoicePurchaseAmount
             , ISNULL (vit.cmanetamount, vij.invoiceamount)                                                                AS InvoicePurchaseAmount_TransCur
             , vit.qty                                                                                                     AS InvoiceQuantity_PurchUOM
             , vit.inventqty                                                                                               AS InvoiceQuantity
             , CASE WHEN vit.taxamount IS NULL THEN NULL ELSE vit.taxamount * vij.exchrate / 100 END                       AS TaxAmount
             , ISNULL (vit.taxamount, vij.sumtax)                                                                          AS TaxAmount_TransCur
             , vit.purchprice * vij.exchrate / 100                                                                         AS BaseUnitPrice
             , vit.purchprice                                                                                              AS BaseUnitPrice_TransCur
             , vit.cmanetprice * vij.exchrate / 100                                                                        AS TotalUnitPrice
             , vit.cmanetprice                                                                                             AS TotalUnitPrice_TransCur
             , vit.priceunit                                                                                               AS PriceUnit
             , CAST(vij.createddatetime AS DATE)                                                                          AS CreatedDate
             , CAST(vij.duedate AS DATE)                                                                                   AS DueDate
             , vij.recid                                                                                                  AS _RECID1
             , ISNULL (vit.recid, 0)                                                                                      AS _RecID2
             , 1                                                                                                           AS _SourceID

          FROM {{ ref('vendinvoicejour') }}        vij
          LEFT JOIN {{ ref('vendinvoicetrans') }}  vit
            ON vit.dataareaid         = vij.dataareaid
           AND vit.purchid             = vij.purchid
           AND vit.invoiceid           = vij.invoiceid
           AND vit.invoicedate         = vij.invoicedate
           AND vit.numbersequencegroup = vij.numbersequencegroup
           AND vit.internalinvoiceid   = vij.internalinvoiceid
          LEFT JOIN {{ ref('inventdim') }}         id
            ON id.dataareaid          = vit.dataareaid
           AND id.inventdimid          = vit.inventdimid
          LEFT JOIN {{ ref('purchline') }}         pl
            ON pl.dataareaid          = vit.dataareaid
           AND pl.inventtransid        = vit.inventtransid
           AND pl.itemid               = vit.itemid
          LEFT JOIN {{ ref('purchtable') }}        ph
            ON ph.dataareaid          = pl.dataareaid
           AND ph.purchid              = pl.purchid
           AND ph.purchstatus          <> 4 
          LEFT JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid         = vit.dataareaid
           AND ito.inventtransid       = vit.inventtransid;
),
purchaseinvoiceline_factcharge AS (
    SELECT dvil.PurchaseInvoiceLineKey          AS PurchaseInvoiceLineKey
             , SUM (crg.IncludedCharge)             AS IncludedCharge
             , SUM (crg.IncludedCharge_TransCur)    AS IncludedCharge_TransCur
             , SUM (crg.AdditionalCharge)           AS AdditionalCharge
             , SUM (crg.AdditionalCharge_TransCur)  AS AdditionalCharge_TransCur
             , SUM (crg.NonBillableCharge)          AS NonBillableCharge
             , SUM (crg.NonBillableCharge_TransCur) AS NonBillableCharge_TransCur

          FROM silver.cma_PurchaseInvoiceLineCharge_Fact crg


         INNER JOIN silver.cma_PurchaseInvoiceLine       dvil
            ON dvil.PurchaseInvoiceLineKey = crg.PurchaseInvoiceLineKey
         GROUP BY dvil.PurchaseInvoiceLineKey;
),
purchaseinvoiceline_factline AS (
    SELECT dvil.PurchaseInvoiceLineKey                                                                                AS PurchaseInvoiceLineKey
             , dpi.PurchaseInvoiceKey                                                                                     AS PurchaseInvoiceKey
             , le.LegalEntityKey                                                                                          AS LegalEntityKey
             , cd.CashDiscountKey                                                                                         AS CashDiscountKey
             , cur.CurrencyKey                                                                                            AS CurrencyKey
             , tm.DeliveryTermKey                                                                                         AS DeliveryTermKey
             , dm.DeliveryModeKey                                                                                         AS DeliveryModeKey
             , pa.PaymentTermKey                                                                                          AS PaymentTermKey
             , tg.TaxGroupKey                                                                                             AS TaxGroupKey
             , pur.UOMKey                                                                                                 AS PurchaseUOMKey
             , pu.UOMKey                                                                                                  AS PricingUOMKey
             , vou.VoucherKey                                                                                             AS VoucherKey
             , da.AddressKey                                                                                              AS DeliveryAddressKey
             , it.LotKey                                                                                                  AS LotKey
             , ds.InventorySiteKey                                                                                        AS InventorySiteKey
             , dd.DateKey                                                                                                 AS InvoiceDateKey
             , dd1.DateKey                                                                                                AS CreatedDateKey
             , dd2.DateKey                                                                                                AS DueDateKey
             , fd1.FinancialKey                                                                                           AS FinancialKey
             , dv2.VendorKey                                                                                              AS InvoiceVendorKey
             , ISNULL (dp.ProductKey, -1)                                                                                 AS ProductKey
             , pit.PurchaseTypeKey                                                                                        AS PurchaseTypeKey
             , dpl.PurchaseOrderLineKey                                                                                   AS PurchaseOrderLineKey
             , dv.VendorKey                                                                                               AS VendorKey
             , dw.WarehouseKey                                                                                            AS WarehouseKey
             , ts.LegalEntityID                                                                                           AS LegalEntityID
             , dp.ProductID                                                                                               AS ProductID
             , le.AccountingCurrencyID                                                                                    AS AccountingCurrencyID
             , le.TransExchangeRateType                                                                                   AS TransExchangeRateType
             , dp.InventoryUOM                                                                                            AS InventoryUnit
             , ts.PurchaseUnit                                                                                            AS PurchaseUnit
             , ts.BaseAmount                                                                                              AS BaseAmount
             , ts.BaseAmount_TransCur                                                                                     AS BaseAmount_TransCur
             , ISNULL ((ca.IncludedCharge + ca.AdditionalCharge), 0)                                                      AS VendorCharge
             , ISNULL ((ca.IncludedCharge_TransCur + ca.AdditionalCharge_TransCur), 0)                                    AS VendorCharge_TransCur
             , ((CASE WHEN ts.BaseUnitPrice = 0
                      THEN CASE WHEN ts.InvoicePurchaseAmount = 0
                                THEN 0
                                ELSE ((ts.NetAmount) / ISNULL (NULLIF(ts.InvoiceQuantity_PurchUOM, 0), 1)) END
                      ELSE ts.BaseUnitPrice / (ISNULL (NULLIF(ts.PriceUnit, 0), 1)) END)
                * (CASE WHEN ts.InvoiceQuantity_PurchUOM = 0 THEN 1 ELSE ts.InvoiceQuantity_PurchUOM END)) - ts.NetAmount AS DiscountAmount
             , ((CASE WHEN ts.BaseUnitPrice_TransCur = 0
                      THEN CASE WHEN ts.InvoicePurchaseAmount_TransCur = 0
                                THEN 0
                                ELSE ((ts.NetAmount_TransCur) / ISNULL (NULLIF(ts.InvoiceQuantity_PurchUOM, 0), 1)) END
                      ELSE ts.BaseUnitPrice_TransCur / ISNULL (NULLIF(ts.PriceUnit, 0), 1) END)
                * (CASE WHEN ts.InvoiceQuantity_PurchUOM = 0 THEN 1 ELSE ts.InvoiceQuantity_PurchUOM END))
               - ts.NetAmount_TransCur                                                                                    AS DiscountAmount_TransCur
             , ca.IncludedCharge                                                                                          AS IncludedCharge
             , ca.IncludedCharge_TransCur                                                                                 AS IncludedCharge_TransCur
             , ca.AdditionalCharge                                                                                        AS AdditionalCharge
             , ca.AdditionalCharge_TransCur                                                                               AS AdditionalCharge_TransCur
             , ca.NonBillableCharge                                                                                       AS NonBillableCharge
             , ca.NonBillableCharge_TransCur                                                                              AS NonBillableCharge_TransCur
             , ts.NetAmount                                                                                               AS NetAmount
             , ts.NetAmount_TransCur                                                                                      AS NetAmount_TransCur
             , ts.InvoicePurchaseAmount                                                                                   AS InvoicePurchaseAmount
             , ts.InvoicePurchaseAmount_TransCur                                                                          AS InvoicePurchaseAmount_TransCur
             , ts.InvoiceQuantity_PurchUOM                                                                                AS InvoiceQuantity_PurchUOM
             , ts.InvoiceQuantity                                                                                         AS InvoiceQuantity
             , ts.TaxAmount                                                                                               AS TaxAmount
             , ts.TaxAmount_TransCur                                                                                      AS TaxAmount_TransCur
             , ts.BaseUnitPrice                                                                                           AS BaseUnitPrice
             , ts.BaseUnitPrice_TransCur                                                                                  AS BaseUnitPrice_TransCur
             , ts.TotalUnitPrice                                                                                          AS TotalUnitPrice
             , ts.TotalUnitPrice_TransCur                                                                                 AS TotalUnitPrice_TransCur
             , ts.PriceUnit                                                                                               AS PriceUnit
             , ts._RECID1                                                                                                 AS _RecID1
             , ts._RecID2                                                                                                 AS _RECID2
             , ts._SourceID                                                                                               AS _SourceID

          FROM purchaseinvoiceline_factstage                       ts
         INNER JOIN silver.cma_LegalEntity         le
            ON le.LegalEntityID          = ts.LegalEntityID
         INNER JOIN silver.cma_PurchaseInvoice     dpi
            ON dpi._RecID                = ts._RECID1
           AND dpi._SourceID             = 1
          LEFT JOIN silver.cma_Date                dd
            ON dd.Date                   = ts.InvoiceDate
          LEFT JOIN silver.cma_Date                dd1
            ON dd1.Date                  = ts.CreatedDate
          LEFT JOIN silver.cma_Date                dd2
            ON dd2.Date                  = ts.DueDate
          LEFT JOIN silver.cma_Address             da
            ON da._RecID                 = ts.DELIVERYPOSTALADDRESS
           AND da._SourceID              = 1
          LEFT JOIN silver.cma_PurchaseOrderLine   dpl
            ON dpl._RecID                = ts.RecID_PL
           AND dpl._SourceID             = 1
         INNER JOIN silver.cma_PurchaseInvoiceLine dvil
            ON dvil._RecID              = ts._RECID1
           AND dvil._RecID2              = ts._RecID2
           AND dvil._SourceID            = 1
          LEFT JOIN silver.cma_Vendor              dv
            ON dv.LegalEntityID          = ts.LegalEntityID
           AND dv.VendorAccount          = ts.OrderAccount
          LEFT JOIN silver.cma_Vendor              dv2
            ON dv2.LegalEntityID         = ts.LegalEntityID
           AND dv2.VendorAccount         = ts.InvoiceAccount
          LEFT JOIN silver.cma_Product             dp
            ON dp.LegalEntityID          = ts.LegalEntityID
           AND dp.ItemID                 = ts.ItemID
           AND dp.ProductLength          = ts.ProductLength
           AND dp.ProductColor           = ts.ProductColor
           AND dp.ProductWidth           = ts.ProductWidth
           AND dp.ProductConfig          = ts.ProductConfig
          LEFT JOIN silver.cma_InventorySite       ds
            ON ds.LegalEntityID          = ts.LegalEntityID
           AND ds.InventorySiteID        = ts.SiteID
          LEFT JOIN silver.cma_Warehouse           dw
            ON dw.LegalEntityID          = ts.LegalEntityID
           AND dw.WarehouseID            = ts.WarehouseID
          LEFT JOIN silver.cma_Financial           fd1
            ON fd1._RecID                = ts.DEFAULTDIMENSION
           AND fd1._SourceID             = 1
          LEFT JOIN silver.cma_Voucher             vou
            ON vou.LegalEntityID         = ts.LegalEntityID
           AND vou.VoucherID             = ts.VoucherID
          LEFT JOIN silver.cma_Lot                 it
            ON it._RecID                 = ts.RecID_ITO
           AND it._SourceID              = 1
          LEFT JOIN silver.cma_DeliveryMode        dm
            ON dm.LegalEntityID          = ts.LegalEntityID
           AND dm.DeliveryModeID         = ts.DeliveryModeID
          LEFT JOIN silver.cma_DeliveryTerm        tm
            ON tm.LegalEntityID          = ts.LegalEntityID
           AND tm.DeliveryTermID         = ts.DeliveryTermID
          LEFT JOIN silver.cma_PaymentTerm         pa
            ON pa.LegalEntityID          = ts.LegalEntityID
           AND pa.PaymentTermID          = ts.PaymentTermID
          LEFT JOIN silver.cma_TaxGroup            tg
            ON tg.LegalEntityID          = ts.LegalEntityID
           AND tg.TaxGroupID             = ts.TaxGroupID
          LEFT JOIN silver.cma_UOM                 pu
            ON pu.UOM                    = ts.PricingUnit
          LEFT JOIN silver.cma_UOM                 pur
            ON pur.UOM                   = ts.PurchaseUnit
          LEFT JOIN silver.cma_CashDiscount        cd
            ON cd.LegalEntityID          = ts.LegalEntityID
           AND cd.CashDiscountID         = ts.CashDiscountID
          LEFT JOIN silver.cma_PurchaseType        pit
            ON pit.PurchaseTypeID        = ts.PurchaseTypeID
          LEFT JOIN silver.cma_Currency            cur
            ON cur.CurrencyID            = ts.CurrencyID
          LEFT JOIN purchaseinvoiceline_factcharge                 ca
            ON ca.PurchaseInvoiceLineKey = dvil.PurchaseInvoiceLineKey;
)
SELECT DISTINCT tl.PurchaseInvoiceLineKey
         , tl.PurchaseInvoiceKey
         , tl.CashDiscountKey
         , tl.CreatedDateKey
         , tl.CurrencyKey
         , tl.DeliveryAddressKey
         , tl.DeliveryModeKey
         , tl.DeliveryTermKey
         , tl.DueDateKey
         , tl.FinancialKey
         , tl.InvoiceDateKey
         , tl.InvoiceVendorKey
         , tl.LegalEntityKey
         , tl.LotKey
         , tl.PaymentTermKey
         , tl.PricingUOMKey
         , tl.ProductKey
         , tl.PurchaseOrderLineKey
         , tl.PurchaseTypeKey
         , tl.PurchaseUOMKey
         , tl.InventorySiteKey
         , tl.TaxGroupKey
         , tl.VendorKey
         , tl.VoucherKey
         , tl.WarehouseKey
         , tl.AdditionalCharge
         , tl.AdditionalCharge_TransCur
         , tl.BaseAmount
         , tl.BaseAmount_TransCur
         , tl.BaseUnitPrice
         , tl.BaseUnitPrice_TransCur
         , CASE WHEN tl.DiscountAmount < 0 THEN tl.DiscountAmount * -1 ELSE tl.DiscountAmount END                       AS DiscountAmount
         , CASE WHEN tl.DiscountAmount_TransCur < 0 THEN tl.DiscountAmount_TransCur * -1 ELSE
                                                                                         tl.DiscountAmount_TransCur END AS DiscountAmount_TransCur
         , tl.IncludedCharge
         , tl.IncludedCharge_TransCur
         , ISNULL (tl.BaseAmount, 0) + ISNULL (tl.VendorCharge, 0) + ISNULL (tl.DiscountAmount, 0)
           + ISNULL (tl.TaxAmount, 0)                                                                                   AS InvoiceTotalAmount
         , ISNULL (tl.BaseAmount_TransCur, 0) + ISNULL (tl.VendorCharge_TransCur, 0)
           + ISNULL (tl.DiscountAmount_TransCur, 0) + ISNULL (tl.TaxAmount_TransCur, 0)                                 AS InvoiceTotalAmount_TransCur
         , tl.InvoiceQuantity
         , tl.InvoiceQuantity_PurchUOM
         , tl.InvoiceQuantity_PurchUOM * ISNULL (vuc.factor, 0)                                                         AS InvoiceQuantity_LB
         , ROUND (tl.InvoiceQuantity_PurchUOM * ISNULL (vuc1.factor, 0), 0)                                             AS InvoiceQuantity_PC

         , tl.InvoiceQuantity_PurchUOM * ISNULL (vuc3.factor, 0)                                                        AS InvoiceQuantity_FT

         , tl.InvoiceQuantity_PurchUOM * ISNULL (vuc5.factor, 0)                                                        AS InvoiceQuantity_SQIN
         , tl.NetAmount
         , tl.NetAmount_TransCur
         , tl.NonBillableCharge
         , tl.NonBillableCharge_TransCur
         , tl.PriceUnit
         , tl.TaxAmount
         , tl.TaxAmount_TransCur
         , tl.InvoicePurchaseAmount
         , tl.InvoicePurchaseAmount_TransCur
         , tl.TotalUnitPrice
         , tl.TotalUnitPrice_TransCur
         , tl.VendorCharge
         , tl.VendorCharge_TransCur
         , tl._RecID1
         , tl._RECID2
         , tl._SourceID
         ,  CURRENT_TIMESTAMP  AS  _CreatedDate
         , CURRENT_TIMESTAMP AS _ModifiedDate

      FROM purchaseinvoiceline_factline                    tl
      LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc
        ON vuc.legalentitykey  = tl.LegalEntityKey
       AND vuc.productkey      = tl.ProductKey
       AND vuc.fromuomkey      = tl.PurchaseUOMKey
    -- AND vuc.touom           = 'LB'
      LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc1
        ON vuc1.legalentitykey = tl.LegalEntityKey
       AND vuc1.productkey     = tl.ProductKey
       AND vuc1.fromuomkey     = tl.PurchaseUOMKey
    -- AND vuc1.touom          = 'PC'

      LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc3
        ON vuc3.legalentitykey = tl.LegalEntityKey
       AND vuc3.productkey     = tl.ProductKey
       AND vuc3.fromuomkey     = tl.PurchaseUOMKey
    -- AND vuc3.touom          = 'FT'





      LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc5
        ON vuc5.legalentitykey = tl.LegalEntityKey
       AND vuc5.productkey     = tl.ProductKey
       AND vuc5.fromuomkey     = tl.PurchaseUOMKey
    -- AND vuc5.touom          = 'SQIN';
