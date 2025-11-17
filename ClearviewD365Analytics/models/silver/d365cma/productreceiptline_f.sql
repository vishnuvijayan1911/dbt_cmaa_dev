{{ config(materialized='table', tags=['silver'], alias='productreceiptline_fact') }}

-- Source file: cma/cma/layers/_base/_silver/productreceiptline_f/productreceiptline_f.py
-- Root method: ProductreceiptlineFact.productreceiptline_factdetail [ProductReceiptLine_FactDetail]
-- Inlined methods: ProductreceiptlineFact.productreceiptline_factreceive [ProductReceiptLine_FactReceive], ProductreceiptlineFact.productreceiptline_factreceiveddnotinvoiced [ProductReceiptLine_FactReceiveddNotInvoiced], ProductreceiptlineFact.productreceiptline_factstage [ProductReceiptLine_FactStage], ProductreceiptlineFact.productreceiptline_factdetailmain [ProductReceiptLine_FactDetailMain]
-- external_table_name: ProductReceiptLine_FactDetail
-- schema_name: temp

WITH
productreceiptline_factreceive AS (
    SELECT vpst.recid                                                                                                    AS RecID_VPST
             , SUM (
                   ISNULL (NULLIF ((it.costamountposted), 0), (it.qty * (vpst.valuemst / ISNULL (NULLIF (vpst.qty, 0), 1))))) AS ReceiveAmount

          FROM {{ ref('vendpackingsliptrans') }}   vpst
         INNER JOIN {{ ref('inventtrans') }}       it
            ON it.dataareaid        = vpst.dataareaid
           AND it.packingslipid      = vpst.packingslipid
           AND it.itemid             = vpst.itemid
           AND it.voucherphysical    = vpst.costledgervoucher
         INNER JOIN {{ ref('inventtransorigin') }} ITO
            ON ITO.recid            = it.inventtransorigin
           AND ITO.referencecategory = 3
           AND ITO.inventtransid     = vpst.inventtransid
         GROUP BY vpst.recid;
),
productreceiptline_factreceiveddnotinvoiced AS (
    SELECT SUM (it.qty)                                                                           AS ReceivedNotInvoicedQuantity
             , MAX (CASE WHEN it.statusissue IN ( 2 ) OR it.statusreceipt IN ( 2 ) THEN 1 ELSE 0 END) AS ReceivedNotInvoicedlineCount
             , MAX (vpst.recid)                                                                      AS RecID_VPST

          FROM {{ ref('vendpackingsliptrans') }} vpst


         INNER JOIN {{ ref('inventtrans') }}     it
            ON it.dataareaid     = vpst.dataareaid
           AND it.packingslipid   = vpst.packingslipid
           AND it.itemid          = vpst.itemid
           AND it.voucherphysical = vpst.costledgervoucher
         WHERE it.statusissue IN ( 2 )
            OR it.statusreceipt IN ( 2 )
         GROUP BY vpst.recid;
),
productreceiptline_factstage AS (
    SELECT DISTINCT
               vt.packingslipid                                                       AS PackingSlipID
             , vj.deliverypostaladdress                                               AS DeliveryPostalAddress
             , pl.defaultdimension                                                    AS DefaultDimension
             , pl.currencycode                                                        AS CurrencyID
             , vt.dataareaid                                                         AS LegalEntityID
             , vj.orderaccount                                                        AS OrderAccount
             , vj.invoiceaccount                                                      AS InvoiceAccount
             , vt.deliverydate                                                        AS ReceiptDate
             , vj.documentdate                                                        AS DocumentDate
             , vt.itemid                                                              AS ItemID
             , ito.recid                                                             AS RecID_ITO
             , vj.dlvmode                                                             AS DeliveryModeID
             , vj.dlvterm                                                             AS DeliveryTermID
             , CASE WHEN vj.purchasetype IN ( 3, 4 ) THEN vj.purchasetype ELSE -1 END AS PurchaseTypeID
             , pl.purchunit                                                 AS PurchaseUOM
             , vt.cmapriceuom                                                 AS PricingUOM
             , vt.costledgervoucher                                                   AS VoucherID
             , id.inventcolorid                                                       AS ProductLength
             , id.inventsizeid                                                        AS ProductWidth
             , id.inventstyleid                                                       AS ProductColor
             , id.configid                                                            AS ProductConfig
             , id.inventsiteid                                                        AS SiteID
             , id.inventlocationid                                                    AS WarehouseID
             , im.unitid                                                      AS InventoryUnit
             , tr.ReceiveAmount                                                       AS ReceiveAmount
             , vt.qty                                                                 AS ReceivedQuantity_PurchUOM
             , vt.inventqty                                                           AS ReceivedQuantity
             , vt.remain                                                              AS RemainingQuantity_PurchUOM
             , vt.remaininvent                                                        AS RemainingQuantity
             , rni.ReceivedNotInvoicedQuantity                                        AS ReceivedNotInvoicedQuantity
             , rni.ReceivedNotInvoicedlineCount                                       AS ReceivedNotInvoicedlineCount
             , CASE WHEN pl.purchasetype = 3
                     AND YEAR (vt.deliverydate) > 1900
                    THEN DATEDIFF (DAY, vt.deliverydate, SYSDATETIME ()) END          AS ReceivedNotInvoicedDays
             , pl.purchprice                                                          AS BaseUnitPrice_TransCur
             , pl.priceunit                                                           AS PriceUnit
             , vt.valuemst                                                            AS TotalAmount
             , pl.recid                                                              AS RecID_PL
             , vt.recid                                                              AS _RecID
             , 1                                                                      AS _SourceID

          FROM {{ ref('vendpackingsliptrans') }}     vt
         INNER JOIN {{ ref('vendpackingslipjour') }} vj
            ON vj.recid         = vt.vendpackingslipjour
         INNER JOIN {{ ref('purchline') }}           pl
            ON pl.dataareaid    = vt.dataareaid
           AND pl.purchid        = vt.origpurchid
           AND pl.inventtransid  = vt.inventtransid
           AND pl.itemid         = vt.itemid
         INNER JOIN {{ ref('inventdim') }}           id
            ON id.dataareaid    = vt.dataareaid
           AND id.inventdimid    = vt.inventdimid
          LEFT JOIN {{ ref('inventtablemodule') }}   im
            ON im.dataareaid    = pl.dataareaid
           AND im.itemid         = pl.itemid
           AND im.moduletype     = 0
          LEFT JOIN {{ ref('inventtransorigin') }}   ito
            ON ito.dataareaid   = pl.dataareaid
           AND ito.inventtransid = pl.inventtransid
          LEFT JOIN productreceiptline_factreceive                tr
            ON tr.RecID_VPST     = vt.recid
          LEFT JOIN productreceiptline_factreceiveddnotinvoiced   rni
            ON rni.RecID_VPST    = vt.recid;
),
productreceiptline_factdetailmain AS (
    SELECT dpl.ProductReceiptLineKey
             , cur.CurrencyKey                                                           AS CurrencyKey
             , da.AddressKey                                                             AS DeliveryAddressKey
             , dm.DeliveryModeKey                                                        AS DeliveryModeKey
             , tm.DeliveryTermKey                                                        AS DeliveryTermKey
             , fd1.FinancialKey                                                          AS FinancialKey
             , dd1.DateKey                                                               AS DocumentDateKey
             , le.LegalEntityKey                                                         AS LegalEntityKey
             , ISNULL (dp.ProductKey, -1)                                                AS ProductKey
             , dpol.PurchaseOrderLineKey                                                 AS PurchaseOrderLineKey
             , pt.PurchaseTypeKey                                                        AS PurchaseTypeKey
             , pu.UOMKey                                                                 AS PricingUOMKey
             , pur.UOMKey                                                                AS PurchaseUOMKey
             , dd.DateKey                                                                AS ReceiptDateKey
             , dv.VendorKey                                                              AS VendorKey
             , vou.VoucherKey                                                            AS VoucherKey
             , dv2.VendorKey                                                             AS InvoiceVendorKey
             , it.lotkey                                                                 AS LotKey
             , dis.InventorySiteKey                                                      AS InventorySiteKey
             , dw.WarehouseKey                                                           AS WarehouseKey
             , ts.ReceiveAmount                                                          AS ReceivedAmount
             , ts.ReceiveAmount * ISNULL (ex1.ExchangeRate, 1)                           AS ReceivedAmount_TransCur
             , ts.ReceivedQuantity_PurchUOM                                              AS ReceivedQuantity_PurchUOM
             , ts.ReceivedQuantity                                                       AS ReceivedQuantity
             , ts.ReceivedNotInvoicedQuantity * ISNULL (vuc1.factor, 0)                  AS ReceivedNotInvoicedQuantity_PurchUOM
             , ts.ReceivedNotInvoicedQuantity                                            AS ReceivedNotInvoicedQuantity
             , ts.ReceivedNotInvoicedlineCount                                           AS ReceivedNotInvoicedlineCount
             , ts.ReceivedNotInvoicedDays                                                AS ReceivedNotInvoicedDays
             , (ISNULL (ts.ReceivedNotInvoicedQuantity, 0) * ts.BaseUnitPrice_TransCur
                / (ISNULL (NULLIF (ts.PriceUnit, 0), 1)))                                AS ReceivedNotInvoicedAmount
             , (ISNULL (ts.ReceivedNotInvoicedQuantity, 0) * ts.BaseUnitPrice_TransCur
                / (ISNULL (NULLIF (ts.PriceUnit, 0), 1))) * ISNULL (ex1.ExchangeRate, 1) AS ReceivedNotInvoicedAmount_TransCur
             , ts.RemainingQuantity_PurchUOM                                             AS RemainingQuantity_PurchUOM
             , ts.RemainingQuantity                                                      AS RemainingQuantity
             , ts.TotalAmount                                                            AS TotalAmount
             , ts.TotalAmount * ISNULL (ex1.ExchangeRate, 1)                             AS TotalAmount_TransCur
             , ts.LegalEntityID                                                          AS LegalEntityID
             , ts.InventoryUnit                                                          AS InventoryUnit
             , ts._SourceID                                                              AS _SourceID
             , ts._RecID                                                                 AS _RecID

          FROM productreceiptline_factstage                      ts
         INNER JOIN {{ ref('legalentity_d') }}        le
            ON le.LegalEntityID     = ts.LegalEntityID
         INNER JOIN {{ ref('productreceiptline_d') }} dpl
            ON dpl._RecID           = ts._RecID
           AND dpl._SourceID        = 1
          LEFT JOIN {{ ref('date_d') }}               dd
            ON dd.Date              = ts.ReceiptDate
          LEFT JOIN {{ ref('date_d') }}               dd1
            ON dd1.Date             = ts.DocumentDate
          LEFT JOIN {{ ref('address_d') }}            da
            ON da._RecID            = ts.DeliveryPostalAddress
           AND da._SourceID         = 1
          LEFT JOIN {{ ref('product_d') }}            dp
            ON dp.LegalEntityID     = ts.LegalEntityID
           AND dp.ItemID            = ts.ItemID
           AND dp.ProductLength     = ts.ProductLength
           AND dp.ProductColor      = ts.ProductColor
           AND dp.ProductWidth      = ts.ProductWidth
           AND dp.ProductConfig     = ts.ProductConfig
          LEFT JOIN {{ ref('purchaseorderline_d') }}  dpol
            ON dpol._RecID          = ts.RecID_PL
           AND dpol._SourceID       = 1
          LEFT JOIN {{ ref('vendor_d') }}             dv
            ON dv.LegalEntityID     = ts.LegalEntityID
           AND dv.VendorAccount     = ts.OrderAccount
          LEFT JOIN {{ ref('vendor_d') }}             dv2
            ON dv2.LegalEntityID    = ts.LegalEntityID
           AND dv2.VendorAccount    = ts.InvoiceAccount
          LEFT JOIN {{ ref('warehouse_d') }}          dw
            ON dw.LegalEntityID     = ts.LegalEntityID
           AND dw.WarehouseID       = ts.WarehouseID
          LEFT JOIN {{ ref('inventorysite_d') }}      dis
            ON dis.LegalEntityID    = ts.LegalEntityID
           AND dis.InventorySiteID  = ts.SiteID
          LEFT JOIN {{ ref('lot_d') }}                it
            ON it._recid            = ts.RecID_ITO
           AND it._sourceid         = 1
          LEFT JOIN {{ ref('financial_d') }}          fd1
            ON fd1._RecID           = ts.DefaultDimension
           AND fd1._SourceID        = 1
          LEFT JOIN {{ ref('deliverymode_d') }}       dm
            ON dm.LegalEntityID     = ts.LegalEntityID
           AND dm.DeliveryModeID    = ts.DeliveryModeID
          LEFT JOIN {{ ref('deliveryterm_d') }}       tm
            ON tm.LegalEntityID     = ts.LegalEntityID
           AND tm.DeliveryTermID    = ts.DeliveryTermID
          LEFT JOIN {{ ref('uom_d') }}                pu
            ON pu.UOM               = ts.PricingUOM
          LEFT JOIN {{ ref('uom_d') }}                pur
            ON pur.UOM              = ts.PurchaseUOM
          LEFT JOIN {{ ref('purchasetype_d') }}       pt
            ON pt.PurchaseTypeID    = ts.PurchaseTypeID
          LEFT JOIN {{ ref('voucher_d') }}            vou
            ON vou.LegalEntityID    = ts.LegalEntityID
           AND vou.VoucherID        = ts.VoucherID
          LEFT JOIN {{ ref('currency_d') }}           cur
            ON cur.CurrencyID       = ts.CurrencyID
          LEFT JOIN {{ ref('exchangerate_f') }}  ex1
            ON ex1.ExchangeDateKey  = dd.DateKey
           AND ex1.FromCurrencyID   = le.AccountingCurrencyID
           AND ex1.ToCurrencyID     = ts.CurrencyID
           AND ex1.ExchangeRateType = le.TransExchangeRateType
          LEFT JOIN {{ ref('vwuomconversion') }}    vuc1
            ON vuc1.legalentityid   = ts.LegalEntityID
           AND vuc1.productkey      = dp.ProductKey
           AND vuc1.fromuom         = dp.InventoryUOM
           AND vuc1.touom           = ts.PurchaseUOM;
)
SELECT DISTINCT td.ProductReceiptLineKey
         , td.CurrencyKey
         , td.DeliveryAddressKey
         , td.DeliveryModeKey
         , td.DeliveryTermKey
         , td.FinancialKey
         , td.DocumentDateKey
         , td.InvoiceVendorKey
         , td.LegalEntityKey
         , td.LotKey
         , td.ProductKey
         , td.PurchaseOrderLineKey
         , td.PurchaseTypeKey
         , td.PricingUOMKey
         , td.PurchaseUOMKey
         , td.ReceiptDateKey
         , td.InventorySiteKey
         , td.VendorKey
         , td.VoucherKey
         , td.WarehouseKey
         , td.ReceivedAmount
         , td.ReceivedAmount_TransCur
         , td.ReceivedQuantity_PurchUOM
         , td.ReceivedQuantity_PurchUOM * ISNULL (vuc.factor, 0)                        AS ReceivedQuantity_LB
         , ROUND (td.ReceivedQuantity_PurchUOM * ISNULL (vuc1.factor, 0), 0)            AS ReceivedQuantity_PC

         , td.ReceivedQuantity_PurchUOM * ISNULL (vuc3.factor, 0)                       AS ReceivedQuantity_FT

         , td.ReceivedQuantity_PurchUOM * ISNULL (vuc5.factor, 0)                       AS ReceivedQuantity_SQIN
         , td.ReceivedQuantity
         , td.ReceivedNotInvoicedQuantity_PurchUOM
         , td.ReceivedNotInvoicedQuantity_PurchUOM * ISNULL (vuc.factor, 0)             AS ReceivedNotInvoicedQuantity_LB
         , ROUND (td.ReceivedNotInvoicedQuantity_PurchUOM * ISNULL (vuc1.factor, 0), 0) AS ReceivedNotInvoicedQuantity_PC

         , td.ReceivedNotInvoicedQuantity_PurchUOM * ISNULL (vuc3.factor, 0)            AS ReceivedNotInvoicedQuantity_FT

         , td.ReceivedNotInvoicedQuantity_PurchUOM * ISNULL (vuc5.factor, 0)            AS ReceivedNotInvoicedQuantity_SQIN
         , td.ReceivedNotInvoicedQuantity
         , td.ReceivedNotInvoicedAmount
         , td.ReceivedNotInvoicedAmount_TransCur
         , td.ReceivedNotInvoicedlineCount
         , td.ReceivedNotInvoicedDays
         , td.RemainingQuantity_PurchUOM
         , td.RemainingQuantity_PurchUOM * ISNULL (vuc.factor, 0)                       AS RemainingQuantity_LB
         , ROUND (td.RemainingQuantity_PurchUOM * ISNULL (vuc1.factor, 0), 0)           AS RemainingQuantity_PC

         , td.RemainingQuantity_PurchUOM * ISNULL (vuc3.factor, 0)                      AS RemainingQuantity_FT

         , td.RemainingQuantity_PurchUOM * ISNULL (vuc5.factor, 0)                      AS RemainingQuantity_SQIN
         , td.RemainingQuantity
         , td.TotalAmount
         , td.TotalAmount_TransCur
         , td._SourceID
         , td._RecID
         , CURRENT_TIMESTAMP AS _CreatedDate
         , CURRENT_TIMESTAMP AS _ModifiedDate 

      FROM productreceiptline_factdetailmain              td
      LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc
        ON vuc.legalentitykey  = td.LegalEntityKey
       AND vuc.productkey      = td.ProductKey
       AND vuc.fromuomkey      = td.PurchaseUOMKey
    -- AND vuc.touom           = 'LB'
      LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc1
        ON vuc1.legalentitykey = td.LegalEntityKey
       AND vuc1.productkey     = td.ProductKey
       AND vuc1.fromuomkey     = td.PurchaseUOMKey
    -- AND vuc1.touom          = 'PC'
      LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc3
        ON vuc3.legalentitykey = td.LegalEntityKey
       AND vuc3.productkey     = td.ProductKey
       AND vuc3.fromuomkey     = td.PurchaseUOMKey
    -- AND vuc3.touom          = 'FT'      
      LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc5
        ON vuc5.legalentitykey = td.LegalEntityKey
       AND vuc5.productkey     = td.ProductKey
       AND vuc5.fromuomkey     = td.PurchaseUOMKey
    -- AND vuc5.touom          = 'SQIN';
