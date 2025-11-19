{{ config(materialized='table', tags=['silver'], alias='glcostdetail_fact') }}

-- Source file: cma/cma/layers/_base/_silver/glcostdetail_f/glcostdetail_f.py
-- Root method: GlcostdetailFact.glcostdetail_factdetail [GLCostDetail_FactDetail]
-- Inlined methods: GlcostdetailFact.glcostdetail_factglpostingrank [GLCostDetail_FactGLPostingRank], GlcostdetailFact.glcostdetail_factporank [GLCostDetail_FactPORank], GlcostdetailFact.glcostdetail_factpercentoftotal [GLCostDetail_FactPercentOfTotal], GlcostdetailFact.glcostdetail_factstage [GLCostDetail_FactStage]
-- external_table_name: GLCostDetail_FactDetail
-- schema_name: temp

WITH
glcostdetail_factglpostingrank AS (
    SELECT vt.recid                                                                                                                         AS RecID_VT
             , gjae.recid                                                                                                                        AS RecID_GJAE
             , gjae.transactioncurrencyamount                                                                                                     AS GLAmount
             , gjae.postingtype                                                                                                                   AS PostingType
             , vt.transtype                                                                                                                       AS TransType
             , CASE WHEN vt.transtype IN ( 14, 0 )
                    THEN 1
                    ELSE
                    ROW_NUMBER () OVER (PARTITION BY vt.recid
                                            ORDER BY CASE gjae.postingtype WHEN 84 THEN 1 WHEN 236 THEN 2 WHEN 71 THEN 3 WHEN 14
                                                                                                                         THEN 4 WHEN 121
                                                                                                                                THEN 5 ELSE
                                                                                                                                       9 END) END AS PostingRank
             , gjae.ledgeraccount                                                                                                                 AS LedgerAccount
             , gjae.mainaccount                                                                                                                   AS MainAccount
             , gjae.ledgerdimension                                                                                                               AS LedgerDimension

          FROM {{ ref('vendtrans') }}                       vt
         INNER JOIN {{ ref('generaljournalentry') }}        gje
            ON gje.subledgervoucherdataareaid = vt.dataareaid
           AND gje.subledgervoucher           = vt.voucher
         INNER JOIN {{ ref('generaljournalaccountentry') }} gjae
            ON gjae.generaljournalentry       = gje.recid
         WHERE ((vt.transtype IN ( 14, 0 ) AND gjae.postingtype IN ( 84, 236, 71, 14, 121 )) OR vt.transtype = 3)
           AND vt.transdate                                                                                  >= '10/1/2018'
           AND vt.invoice                                                                                    <> '';
),
glcostdetail_factporank AS (
    SELECT a.RecID_VT
             , a.RecID_GJAE
             , a.TransType
             , a.GLAmount
             , a.LedgerAccount
             , a.MainAccount
             , a.PostingType
             , a.LedgerDimension

          FROM glcostdetail_factglpostingrank a
         WHERE a.PostingRank = 1;
),
glcostdetail_factpercentoftotal AS (
    SELECT tpr.RecID_VT                                                                                                      AS RecID_VT
             , tpr.RecID_GJAE                                                                                                    AS RecID_GJAE
             , vij.recid                                                                                                        AS RecID_VIJ
             , vit.recid                                                                                                        AS RecID_VIT
             , vt.dataareaid                                                                                                    AS DATAAREAID
             , vit.inventtransid                                                                                                 AS InventTransID
             , vit.inventdimid                                                                                                   AS InventDimID
             , CASE WHEN tpr.TransType = 3 
                    THEN CASE WHEN SUM (vit.cmanetamount) OVER (PARTITION BY tpr.RecID_VT) = 0
                              THEN 1 / COUNT (1) OVER (PARTITION BY tpr.RecID_VT)
                              ELSE
                              ISNULL (vit.cmanetamount, 0)
                              / ISNULL (SUM (vit.cmanetamount) OVER (PARTITION BY tpr.RecID_VT), 1) END
                    ELSE 
                    CASE WHEN SUM (tpr.GLAmount) OVER (PARTITION BY tpr.RecID_VT) = 0
                         THEN 1 / COUNT (1) OVER (PARTITION BY tpr.RecID_VT)
                         ELSE ISNULL (tpr.GLAmount, 0) / SUM (ISNULL (tpr.GLAmount, 0)) OVER (PARTITION BY tpr.RecID_VT) END END AS PercentOfTotal

          FROM glcostdetail_factporank                   tpr
         INNER JOIN {{ ref('vendtrans') }}        vt
            ON vt.recid               = tpr.RecID_VT
           AND vt.invoice              <> ''
         INNER JOIN {{ ref('vendinvoicejour') }}  vij
            ON vij.dataareaid         = vt.dataareaid
           AND vij.invoiceaccount      = vt.accountnum
           AND vij.invoicedate         = vt.transdate
           AND vij.ledgervoucher       = vt.voucher
           AND vij.invoiceid           = vt.invoice
          LEFT JOIN {{ ref('vendinvoicetrans') }} vit
            ON vit.dataareaid         = vij.dataareaid
           AND vit.purchid             = vij.purchid
           AND vit.invoiceid           = vij.invoiceid
           AND vit.invoicedate         = vij.invoicedate
           AND vit.numbersequencegroup = vij.numbersequencegroup
           AND vit.internalinvoiceid   = vij.internalinvoiceid;
),
glcostdetail_factstage AS (
    SELECT vt.remittanceaddress                                                                                  AS PaymentAddress
             , ISNULL (vij.sumtax, 0)                                                                                AS TaxAmount
             , CAST(ISNULL (vit.linenum, 0) AS INT)                                                                  AS LineNumber
             , vij.payment                                                                                           AS PaymentTermID
             , ptm.numofdays                                                                                         AS PaymentTermDays
             , vt.paymmode                                                                                           AS PaymentModeID
             , ISNULL (vij.orderaccount, vt.accountnum)                                                              AS VendorAccount
             , ISNULL (vij.invoiceaccount, vt.accountnum)                                                            AS InvoiceAccount
             , vt.dataareaid                                                                                        AS LegalEntityID
             , vt.invoice                                                                                            AS Invoice
             , CAST(vt.transdate AS DATE)                                                                            AS TransDate
             , CAST(vt.documentdate AS DATE)                                                                         AS InvoiceDate
             , CAST(vt.approveddate AS DATE)                                                                         AS InvoiceReceiptDate
             , CAST(vt.duedate AS DATE)                                                                              AS InvoiceDueDate
             , CAST(vt.lastsettledate AS DATE)                                                                       AS LastSettleDate
             , CASE WHEN NOT (   vt.lastsettledate = CAST('1/1/1900' AS DATE)
                            OR   vt.duedate = CAST('1/1/1900' AS DATE)
                            OR   vt.closed = CAST('1/1/1900' AS DATE))
                    THEN CASE WHEN vt.lastsettledate <= vt.duedate
                              THEN 0
                              ELSE DATEDIFF (DAY, vt.duedate, vt.lastsettledate) + ISNULL (ptm.numofdays, 0) END END AS WAPDays
             , vt.currencycode                                                                                       AS CurrencyID
             , tpr.LedgerDimension                                                                                   AS DefaultDimension
             , vt.voucher                                                                                            AS VoucherID
             , tpr.PostingType                                                                                       AS PostingTypeID
             , vij.purchid                                                                                           AS PurchaseOrderID
             , vit.itemid                                                                                            AS ItemID
             , vit.procurementcategory                                                                               AS ProcurementCategory
             , id.inventlocationid                                                                                   AS WarehouseID
             , id.inventcolorid                                                                                      AS ProductLength
             , id.inventsizeid                                                                                       AS ProductWidth
             , id.inventstyleid                                                                                      AS ProductColor
             , id.configid                                                                                           AS ProductConfig
             , ivs.siteid                                                                                            AS SiteID
             , vt.transtype                                                                                          AS TransTypeID
             , vt.amountcur * tp.PercentOfTotal * -1                                                                 AS ProratedAmount_TransCur
             , vt.amountmst * tp.PercentOfTotal * -1                                                                 AS ProratedAmount
             , tpr.MainAccount                                                                                       AS RecID_MA
             , pl.recid                                                                                             AS RecID_PL
             , erc.recid                                                                                             AS RecID_ERC
             , tp.RecID_GJAE                                                                                         AS _RecID1
             , tp.RecID_VIT                                                                                          AS _RecID2
             , 1                                                                                                     AS _SourceID

          FROM glcostdetail_factporank                   tpr
         INNER JOIN glcostdetail_factpercentoftotal      tp
            ON tp.RecID_GJAE    = tpr.RecID_GJAE
         INNER JOIN {{ ref('vendtrans') }}        vt
            ON vt.recid        = tpr.RecID_VT
           AND vt.invoice       <> ''
          LEFT JOIN {{ ref('vendinvoicejour') }}  vij
            ON vij.recid       = tp.RecID_VIJ
          LEFT JOIN {{ ref('vendinvoicetrans') }} vit
            ON vit.recid       = tp.RecID_VIT
          LEFT JOIN {{ ref('purchline') }}        pl
            ON pl.dataareaid   = tp.DATAAREAID
           AND pl.inventtransid = tp.InventTransID
          LEFT JOIN {{ ref('paymterm') }}         ptm
            ON ptm.dataareaid  = vij.dataareaid
           AND ptm.paymtermid   = vij.payment
          LEFT JOIN  {{ ref('ecorescategory') }}    erc
            ON erc.recid       = vit.procurementcategory
          LEFT JOIN {{ ref('inventdim') }}        id
            ON id.dataareaid   = tp.DATAAREAID
           AND id.inventdimid   = tp.InventDimID
          LEFT JOIN  {{ ref('inventsite') }}       ivs
            ON ivs.dataareaid  = id.dataareaid
           AND ivs.siteid       = id.inventsiteid;
)
SELECT ROW_NUMBER() OVER (ORDER BY ts._RecID1, ts._RecID2, ts._SourceID) AS GLCostDetailKey
         , da.AddressKey              AS AddressKey
         , cc.CurrencyKey             AS CurrencyKey
         , dd1.DateKey                AS InvoiceDateKey
         , dd2.DateKey                AS InvoiceReceiptDateKey
         , dd3.DateKey                AS InvoiceDueDateKey
         , dfd.LedgerAccountKey       AS LedgerAccountKey
         , dd4.DateKey                AS LastSettleDateKey
         , dlt.LedgerTransTypeKey     AS LedgerTransTypeKey
         , le.LegalEntityKey          AS LegalEntityKey
         , pm.PaymentModeKey          AS PaymentModeKey
         , pt.PaymentTermKey          AS PaymentTermKey
         , pos.PostingTypeKey         AS PostingTypeKey
         , dpc.ProcurementCategoryKey AS ProcurementCategoryKey
         , dp.ProductKey              AS ProductKey
         , dpol.PurchaseOrderLineKey  AS PurchaseOrderLineKey
         , ds.InventorySiteKey        AS InventorySiteKey
         , dd.DateKey                 AS TransDateKey
         , dv.VendorKey               AS VendorKey
         , dv1.VendorKey              AS VendorInvoiceKey
         , dvo.VoucherKey             AS VoucherKey
         , dw.WarehouseKey            AS WarehouseKey
         , ts.ProratedAmount          AS ProratedAmount
         , ts.ProratedAmount_TransCur AS ProratedAmount_TransCur
         , ts.TaxAmount               AS TaxAmount
         , ts.WAPDays                 AS WAPDays
         , ts.Invoice                 AS InvoiceID
         , ts.LineNumber              AS LineNumber
         , ts.PaymentTermDays         AS PaymentTermDays
         , ts._RecID1                 AS _RecID1
         , ts._RecID2                 AS _RecID2
         , ts._SourceID               AS _SourceID

         ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))  AS  _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
      FROM glcostdetail_factstage                       ts
     INNER JOIN {{ ref('legalentity_d') }}         le
        ON le.LegalEntityID      = ts.LegalEntityID
      LEFT JOIN {{ ref('purchaseorderline_d') }}   dpol
        ON dpol._RecID           = ts.RecID_PL
       AND dpol._SourceID        = 1
      LEFT JOIN {{ ref('address_d') }}             da
        ON da._RecID             = ts.PaymentAddress
       AND da._SourceID          = 1
      LEFT JOIN {{ ref('product_d') }}             dp
        ON dp.LegalEntityID      = ts.LegalEntityID
       AND dp.ItemID             = ts.ItemID
       AND dp.ProductLength      = ts.ProductLength
       AND dp.ProductWidth       = ts.ProductWidth
       AND dp.ProductColor       = ts.ProductColor
       AND dp.ProductConfig      = ts.ProductConfig
      LEFT JOIN {{ ref('procurementcategory_d') }} dpc
        ON dpc._RecID            = ts.RecID_ERC
       AND dpc._SourceID         = 1
      LEFT JOIN {{ ref('vendor_d') }}              dv
        ON dv.LegalEntityID      = ts.LegalEntityID
       AND dv.VendorAccount      = ts.VendorAccount
      LEFT JOIN {{ ref('vendor_d') }}              dv1
        ON dv1.LegalEntityID     = ts.LegalEntityID
       AND dv1.VendorAccount     = ts.InvoiceAccount
      LEFT JOIN {{ ref('inventorysite_d') }}       ds
        ON ds.LegalEntityID      = ts.LegalEntityID
       AND ds.InventorySiteID    = ts.SiteID
      LEFT JOIN {{ ref('paymentterm_d') }}         pt
        ON pt.LegalEntityID      = ts.LegalEntityID
       AND pt.PaymentTermID      = ts.PaymentTermID
      LEFT JOIN {{ ref('postingtype_d') }}         pos
        ON pos.PostingTypeID     = ts.PostingTypeID
      LEFT JOIN {{ ref('paymentmode_d') }}         pm
        ON pm.LegalEntityID      = ts.LegalEntityID
       AND pm.PaymentModeID      = ts.PaymentModeID
      LEFT JOIN {{ ref('ledgertranstype_d') }}     dlt
        ON dlt.LedgerTransTypeID = ts.TransTypeID
      LEFT JOIN {{ ref('warehouse_d') }}           dw
        ON dw.LegalEntityID      = ts.LegalEntityID
       AND dw.WarehouseID        = ts.WarehouseID
      LEFT JOIN {{ ref('ledgeraccount_d') }}       dfd
        ON dfd._RecID            = ts.DefaultDimension
      LEFT JOIN {{ ref('voucher_d') }}             dvo
        ON dvo.LegalEntityID     = ts.LegalEntityID
       AND dvo.VoucherID         = ts.VoucherID
      LEFT JOIN {{ ref('date_d') }}                dd
        ON dd.Date               = ts.TransDate
      LEFT JOIN {{ ref('date_d') }}                dd1
        ON dd1.Date              = ts.InvoiceDate
      LEFT JOIN {{ ref('date_d') }}                dd2
        ON dd2.Date              = ts.InvoiceReceiptDate
      LEFT JOIN {{ ref('date_d') }}                dd3
        ON dd3.Date              = ts.InvoiceDueDate
      LEFT JOIN {{ ref('date_d') }}                dd4
        ON dd4.Date              = ts.LastSettleDate
      LEFT JOIN {{ ref('currency_d') }}            cc
        ON cc.CurrencyID         = ts.CurrencyID;
