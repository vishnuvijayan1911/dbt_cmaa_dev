{{ config(materialized='table', tags=['silver'], alias='productreceiptlinetrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/productreceiptlinetrans_f/productreceiptlinetrans_f.py
-- Root method: ProductreceiptlinetransFact.productreceiptlinetrans_factdetail [ProductReceiptLineTrans_FactDetail]
-- Inlined methods: ProductreceiptlinetransFact.productreceiptlinetrans_factordertrans [ProductReceiptLineTrans_FactOrderTrans], ProductreceiptlinetransFact.productreceiptlinetrans_factinventtrans [ProductReceiptLineTrans_FactInventTrans], ProductreceiptlinetransFact.productreceiptlinetrans_factstage [ProductReceiptLineTrans_FactStage], ProductreceiptlinetransFact.productreceiptlinetrans_factratio [ProductReceiptLineTrans_FactRatio], ProductreceiptlinetransFact.productreceiptlinetrans_facttrans [ProductReceiptLineTrans_FactTrans], ProductreceiptlinetransFact.productreceiptlinetrans_factadj [ProductReceiptLineTrans_FactAdj], ProductreceiptlinetransFact.productreceiptlinetrans_facttransadj [ProductReceiptLineTrans_FactTransAdj], ProductreceiptlinetransFact.productreceiptlinetrans_facttransadj2 [ProductReceiptLineTrans_FactTransAdj2], ProductreceiptlinetransFact.productreceiptlinetrans_facttransadj3 [ProductReceiptLineTrans_FactTransAdj3], ProductreceiptlinetransFact.productreceiptlinetrans_facttransadj4 [ProductReceiptLineTrans_FactTransAdj4]
-- external_table_name: ProductReceiptLineTrans_FactDetail
-- schema_name: temp

WITH
productreceiptlinetrans_factordertrans AS (
    SELECT frl.ProductReceiptLineKey
             , polt.PurchaseOrderLineTransKey
             , ROW_NUMBER () OVER (PARTITION BY frl.ProductReceiptLineKey
    ORDER BY polt._RecID2) AS OrderTransRank
          FROM {{ ref('productreceiptline_f') }}          frl
         INNER JOIN {{ ref('purchaseorderlinetrans_f') }} polt
            ON polt.PurchaseOrderLineKey = frl.PurchaseOrderLineKey
         WHERE frl.PurchaseOrderLineKey <> -1;
),
productreceiptlinetrans_factinventtrans AS (
    SELECT it.recid                                                                                    AS RecID_IT
             , MAX (it.dataareaid)                                                                         AS DATAAREAID
             , MAX (it.packingslipid)                                                                       AS PACKINGSLIPID
             , MAX (it.itemid)                                                                              AS ITEMID
             , MAX (ito.inventtransid)                                                                      AS INVENTTRANSID
             , MAX (it.qty)                                                                                 AS QTY_IT
             , MAX (it.statusreceipt)                                                                       AS StatusReceipt
             , MAX (it.statusissue)                                                                         AS StatusIssue
             , MAX (CASE WHEN it.statusissue IN ( 1, 2 ) OR it.statusreceipt IN ( 1, 2 ) THEN 1 ELSE 0 END) AS Received
             , MAX (CASE WHEN it.statusissue IN ( 2 ) OR it.statusreceipt IN ( 2 ) THEN 1 ELSE 0 END)       AS ReceivedNotInvoiced
             , MAX (it.inventdimid)                                                                         AS InventDimID
             , MAX (it.voucherphysical)                                                                     AS VOUCHERPHYSICAL
             , MAX (it.costamountposted)                                                                    AS ReceivedAmount
             , MAX (vpst.recid)                                                                            AS RecID_VPST
             , MAX (vpst.inventqty)                                                                         AS InventQty_VPST

          FROM {{ ref('vendpackingsliptrans') }}   vpst
         INNER JOIN {{ ref('inventtrans') }}       it
            ON it.dataareaid        = vpst.dataareaid
           AND it.packingslipid      = vpst.packingslipid
           AND it.itemid             = vpst.itemid
           AND it.voucherphysical    = vpst.costledgervoucher
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.recid            = it.inventtransorigin
           AND ito.referencecategory = 3
           AND ito.inventtransid     = vpst.inventtransid
         WHERE (it.statusissue IN ( 1, 2, 3, 4, 5, 6 ) OR it.statusreceipt IN ( 1, 2, 3, 4, 5 ))
         GROUP BY it.recid;
),
productreceiptlinetrans_factstage AS (
    SELECT DISTINCT
               tt.RecID_IT                                                                                              AS RecID_IT
             , vpst.recid                                                                                              AS RecID_VPST
             , tt.DATAAREAID                                                                                            AS LegalEntityID
             , pl.currencycode                                                                                          AS CurrencyID
             , tt.StatusReceipt                                                                                         AS StatusReceipt
             , id.inventbatchid                                                                                         AS TagID
             , vpst.itemid                                                                                              AS ItemID
             , pl.priceunit                                                                                             AS PriceUnit
             , tt.ReceivedAmount                                                                                        AS ReceivedAmount
             , (CASE WHEN tt.Received = 1 THEN tt.QTY_IT ELSE 0 END * vpst.qty / ISNULL (NULLIF(vpst.inventqty, 0), 1)) AS ReceivedQuantity_PurchUOM
             , (CASE WHEN tt.Received = 1 THEN tt.QTY_IT ELSE 0 END)                                                    AS ReceivedQuantity
             , (CASE WHEN tt.ReceivedNotInvoiced = 1 THEN tt.QTY_IT ELSE 0 END * vpst.qty
                / ISNULL (NULLIF(vpst.inventqty, 0), 1))                                                                AS ReceivedNotInvoicedQuantity_PurchUOM
             , (CASE WHEN tt.ReceivedNotInvoiced = 1 THEN tt.QTY_IT ELSE 0 END)                                         AS ReceivedNotInvoicedQuantity
             , tt.ReceivedNotInvoiced                                                                                   AS ReceivedNotInvoiceTransCount
             , (tt.QTY_IT - CASE WHEN tt.Received = 1 THEN tt.QTY_IT ELSE 0 END * vpst.qty
                / ISNULL (NULLIF(vpst.inventqty, 0), 1))                                                                AS RemainingQuantity_PurchUOM
             , (tt.QTY_IT - CASE WHEN tt.Received = 1 THEN tt.QTY_IT ELSE 0 END)                                        AS RemainingQuantity
             , pl.recid                                                                                                AS RecID_PL

          FROM productreceiptlinetrans_factinventtrans                  tt
         INNER JOIN {{ ref('vendpackingsliptrans') }} vpst
            ON vpst.recid      = tt.RecID_VPST
         INNER JOIN {{ ref('purchline') }}            pl
            ON pl.dataareaid   = vpst.dataareaid
           AND pl.inventtransid = vpst.inventtransid
           AND pl.itemid        = vpst.itemid
         INNER JOIN {{ ref('inventdim') }}            id
            ON id.dataareaid   = tt.DATAAREAID
           AND id.inventdimid   = tt.InventDimID
         ORDER BY RecID_IT;
),
productreceiptlinetrans_factratio AS (
    SELECT ts.RecID_IT                                                                                       AS RecID_IT
             , CASE WHEN SUM (ts.QTY_IT) OVER (PARTITION BY ts.RecID_VPST) = 0
                    THEN 1 / CAST(ISNULL (NULLIF(COUNT (1) OVER (PARTITION BY ts.RecID_VPST), 0), 1) AS FLOAT)
                    ELSE
                    CAST(ts.QTY_IT AS FLOAT)
                    / CAST(ISNULL (NULLIF(SUM (ts.QTY_IT) OVER (PARTITION BY ts.RecID_VPST), 0), 1) AS FLOAT)END AS PercentOfTotal

          FROM productreceiptlinetrans_factinventtrans ts;
),
productreceiptlinetrans_facttrans AS (
    SELECT frl.ProductReceiptLineKey
             , CAST(frl.RemainingQuantity_PurchUOM * ISNULL (tr.PercentOfTotal, 1) AS NUMERIC(20, 6)) AS RemainingQuantity_PurchUOM
             , CAST(frl.RemainingQuantity * ISNULL (tr.PercentOfTotal, 1) AS NUMERIC(20, 6))          AS RemainingQuantity
             , CAST(frl.RemainingQuantity_LB * ISNULL (tr.PercentOfTotal, 1) AS NUMERIC(20, 6))       AS RemainingQuantity_LB
             , CAST(frl.RemainingQuantity_PC * ISNULL (tr.PercentOfTotal, 1) AS NUMERIC(20, 6))       AS RemainingQuantity_PC

             , CAST(frl.RemainingQuantity_FT * ISNULL (tr.PercentOfTotal, 1) AS NUMERIC(20, 6))       AS RemainingQuantity_FT

             , CAST(frl.RemainingQuantity_SQIN * ISNULL (tr.PercentOfTotal, 1) AS NUMERIC(20, 6))     AS RemainingQuantity_SQIN
             , CAST(frl.ReceivedNotInvoicedAmount * ISNULL (tr.PercentOfTotal, 1) AS MONEY)           AS ReceivedNotInvoicedAmount
             , CAST(frl.ReceivedNotInvoicedAmount_TransCur * ISNULL (tr.PercentOfTotal, 1) AS MONEY)  AS ReceivedNotInvoicedAmount_TransCur
             , ts.ReceivedQuantity_PurchUOM
             , ts.ReceivedQuantity
             , ts.ReceivedAmount
             , ts.ReceivedNotInvoicedQuantity_PurchUOM
             , ts.ReceivedNotInvoicedQuantity
             , ts.ReceivedNotInvoiceTransCount
             , ts.LegalEntityID
             , ts.TagID
             , ts.ItemID
             , ts.RecID_PL
             , ts.CurrencyID
             , ts.StatusReceipt
             , CASE WHEN ROW_NUMBER () OVER (PARTITION BY frl.ProductReceiptLineKey
    ORDER BY ISNULL (ts.RecID_IT, 0)) = 1
                    THEN 1
                    ELSE 0 END                                                                        AS IsProrateAdj
             , ts.RecID_IT
             , ts.RecID_VPST
          FROM {{ ref('productreceiptline_f') }} frl
         INNER JOIN productreceiptlinetrans_factstage                ts
            ON frl._RecID    = ts.RecID_VPST
           AND frl._SourceID = 1
         INNER JOIN productreceiptlinetrans_factratio                 tr
            ON tr.RecID_IT   = ts.RecID_IT;
),
productreceiptlinetrans_factadj AS (
    SELECT CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.RemainingQuantity - SUM (t.RemainingQuantity) OVER (PARTITION BY t.ProductReceiptLineKey)
                         ELSE 0 END AS NUMERIC(20, 6)) AS RemainingQuantityAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.RemainingQuantity_PurchUOM
                              - SUM (t.RemainingQuantity_PurchUOM) OVER (PARTITION BY t.ProductReceiptLineKey)
                         ELSE 0 END AS NUMERIC(20, 6)) AS RemainingQuantity_PurchUOMAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.RemainingQuantity_LB
                              - SUM (t.RemainingQuantity_LB) OVER (PARTITION BY t.ProductReceiptLineKey)
                         ELSE 0 END AS NUMERIC(20, 6)) AS RemainingQuantity_LBAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.RemainingQuantity_PC
                              - SUM (t.RemainingQuantity_PC) OVER (PARTITION BY t.ProductReceiptLineKey)
                         ELSE 0 END AS NUMERIC(20, 6)) AS RemainingQuantity_PCAdj




             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.RemainingQuantity_FT
                              - SUM (t.RemainingQuantity_FT) OVER (PARTITION BY t.ProductReceiptLineKey)
                         ELSE 0 END AS NUMERIC(20, 6)) AS RemainingQuantity_FTAdj




             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.RemainingQuantity_SQIN
                              - SUM (t.RemainingQuantity_SQIN) OVER (PARTITION BY t.ProductReceiptLineKey)
                         ELSE 0 END AS NUMERIC(20, 6)) AS RemainingQuantity_SQINAdj
             , t.RecID_IT
             , t.RecID_VPST
             , fcl.ProductReceiptLineKey

          FROM productreceiptlinetrans_facttrans                           t
         INNER JOIN {{ ref('productreceiptline_f') }} fcl
            ON fcl.ProductReceiptLineKey = t.ProductReceiptLineKey
),
productreceiptlinetrans_facttransadj AS (
    SELECT ProductReceiptLineKey
             , RecID_IT
             , RecID_VPST
             , RemainingQuantityAdj
             , RemainingQuantity_PurchUOMAdj
             , RemainingQuantity_LBAdj
             , RemainingQuantity_PCAdj

             , RemainingQuantity_FTAdj

             , RemainingQuantity_SQINAdj

          FROM productreceiptlinetrans_factadj
         WHERE RemainingQuantityAdj          <> 0
            OR RemainingQuantity_PurchUOMAdj <> 0
            OR RemainingQuantity_LBAdj       <> 0
            OR RemainingQuantity_PCAdj       <> 0;
),
productreceiptlinetrans_facttransadj2 AS (
    SELECT  t.ProductReceiptLineKey
             , t.RemainingQuantity_PurchUOM + ta.RemainingQuantity_PurchUOMAdj AS RemainingQuantity_PurchUOM
             , t.RemainingQuantity + ta.RemainingQuantityAdj                   AS RemainingQuantity
             , t.RemainingQuantity_LB + ta.RemainingQuantity_LBAdj             AS RemainingQuantity_LB
             , t.RemainingQuantity_PC + ta.RemainingQuantity_PCAdj             AS RemainingQuantity_PC
             , t.RemainingQuantity_FT + ta.RemainingQuantity_FTAdj             AS RemainingQuantity_FT
             , t.RemainingQuantity_SQIN + ta.RemainingQuantity_SQINAdj         AS RemainingQuantity_SQIN
             , t.ReceivedNotInvoicedAmount
             , t.ReceivedNotInvoicedAmount_TransCur
             , t.ReceivedQuantity_PurchUOM
             , t.ReceivedQuantity
             , t.ReceivedAmount
             , t.ReceivedNotInvoicedQuantity_PurchUOM
             , t.ReceivedNotInvoicedQuantity
             , t.ReceivedNotInvoiceTransCount
             , t.LegalEntityID
             , t.TagID
             , t.ItemID
             , t.RecID_PL
             , t.CurrencyID
             , t.StatusReceipt
             , t.IsProrateAdj
             , t.RecID_IT
             , t.RecID_VPST
    		 FROM productreceiptlinetrans_facttrans         t
         INNER JOIN productreceiptlinetrans_facttransadj ta
            ON ta.RecID_VPST = t.RecID_VPST
           AND ta.RecID_IT   = t.RecID_IT
),
productreceiptlinetrans_facttransadj3 AS (
    SELECT  t.ProductReceiptLineKey
             , t.RemainingQuantity_PurchUOM
             , t.RemainingQuantity
             , t.RemainingQuantity_LB
             , t.RemainingQuantity_PC
             , t.RemainingQuantity_FT
             , t.RemainingQuantity_SQIN
             , t.ReceivedNotInvoicedAmount
             , t.ReceivedNotInvoicedAmount_TransCur
             , t.ReceivedQuantity_PurchUOM
             , t.ReceivedQuantity
             , t.ReceivedAmount
             , t.ReceivedNotInvoicedQuantity_PurchUOM
             , t.ReceivedNotInvoicedQuantity
             , t.ReceivedNotInvoiceTransCount
             , t.LegalEntityID
             , t.TagID
             , t.ItemID
             , t.RecID_PL
             , t.CurrencyID
             , t.StatusReceipt
             , t.IsProrateAdj
             , t.RecID_IT
             , t.RecID_VPST
    		 FROM productreceiptlinetrans_facttrans         t
         LEFT JOIN productreceiptlinetrans_facttransadj ta
            ON ta.RecID_VPST = t.RecID_VPST
           AND ta.RecID_IT   = t.RecID_IT
    	 WHERE (ta.RecID_VPST IS NULL) OR (ta.RecID_IT IS NULL)
),
productreceiptlinetrans_facttransadj4 AS (
    SELECT * FROM productreceiptlinetrans_facttransadj2
    UNION ALL
    SELECT * FROM productreceiptlinetrans_facttransadj3
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY frl.ProductReceiptLineKey) AS ProductReceiptLineTransKey,
    frl.ProductReceiptLineKey
         , dis.InventoryTransStatusKey                                                                               AS InventoryTransStatusKey
         , dt.TagKey                                                                                                 AS TagKey
         , ISNULL (polt.PurchaseOrderLineTransKey, ot.PurchaseOrderLineTransKey)                                     AS PurchaseOrderLineTransKey
         , ISNULL (ts.ReceivedQuantity_PurchUOM, frl.ReceivedQuantity_PurchUOM)                                      AS ReceivedQuantity_PurchUOM
         , ISNULL (ts.ReceivedQuantity, frl.ReceivedQuantity)                                                        AS ReceivedQuantity
         , ISNULL (ts.ReceivedQuantity, 1) * frl.ReceivedQuantity_LB / ISNULL (NULLIF(frl.ReceivedQuantity, 0), 1)   AS ReceivedQuantity_LB
         , ISNULL (ts.ReceivedQuantity, 1) * frl.ReceivedQuantity_PC / ISNULL (NULLIF(frl.ReceivedQuantity, 0), 1)   AS ReceivedQuantity_PC

         , ISNULL (ts.ReceivedQuantity, 1) * frl.ReceivedQuantity_FT / ISNULL (NULLIF(frl.ReceivedQuantity, 0), 1)   AS ReceivedQuantity_FT

         , ISNULL (ts.ReceivedQuantity, 1) * frl.ReceivedQuantity_SQIN / ISNULL (NULLIF(frl.ReceivedQuantity, 0), 1) AS ReceivedQuantity_SQIN
         , ISNULL (ts.ReceivedAmount, frl.ReceivedAmount)                                                            AS ReceivedAmount
         , ISNULL (ts.ReceivedAmount, frl.ReceivedAmount) * ISNULL (ex.ExchangeRate, 1)                              AS ReceivedAmount_TransCur
         , ISNULL (ts.RemainingQuantity, frl.RemainingQuantity)                                                      AS RemainingQuantity
         , ISNULL (ts.RemainingQuantity_PurchUOM, frl.RemainingQuantity_PurchUOM)                                    AS RemainingQuantity_PurchUOM
         , ISNULL (ts.RemainingQuantity_LB, frl.RemainingQuantity_LB)                                                AS RemainingQuantity_LB
         , ISNULL (ts.RemainingQuantity_PC, frl.RemainingQuantity_PC)                                                AS RemainingQuantity_PC

         , ISNULL (ts.RemainingQuantity_FT, frl.RemainingQuantity_FT)                                                AS RemainingQuantity_FT

         , ISNULL (ts.RemainingQuantity_SQIN, frl.RemainingQuantity_SQIN)                                            AS RemainingQuantity_SQIN
         , ISNULL (ts.ReceivedNotInvoicedAmount, frl.ReceivedNotInvoicedAmount)                                      AS ReceivedNotInvoicedAmount
         , ISNULL (ts.ReceivedNotInvoicedAmount, frl.ReceivedNotInvoicedAmount) * ISNULL (ex.ExchangeRate, 1)        AS ReceivedNotInvoicedAmount_TransCur
         , CASE WHEN ts.ReceivedNotInvoiceTransCount = 1
                THEN ISNULL (ts.ReceivedNotInvoicedQuantity_PurchUOM, frl.ReceivedNotInvoicedQuantity_PurchUOM)
                ELSE 0 END                                                                                           AS ReceivedNotInvoicedQuantity_PurchUOM
         , CASE WHEN ts.ReceivedNotInvoiceTransCount = 1
                THEN ISNULL (ts.ReceivedNotInvoicedQuantity, frl.ReceivedNotInvoicedQuantity)
                ELSE 0 END                                                                                           AS ReceivedNotInvoicedQuantity
         , CASE WHEN ts.ReceivedNotInvoiceTransCount = 1
                THEN ISNULL (ts.ReceivedNotInvoicedQuantity, 1) * frl.ReceivedNotInvoicedQuantity_LB
                     / ISNULL (NULLIF(frl.ReceivedNotInvoicedQuantity, 0), 1)
                ELSE 0 END                                                                                           AS ReceivedNotInvoicedQuantity_LB
         , CASE WHEN ts.ReceivedNotInvoiceTransCount = 1
                THEN ISNULL (ts.ReceivedNotInvoicedQuantity, 1) * frl.ReceivedNotInvoicedQuantity_PC
                     / ISNULL (NULLIF(frl.ReceivedNotInvoicedQuantity, 0), 1)
                ELSE 0 END                                                                                           AS ReceivedNotInvoicedQuantity_PC




         , CASE WHEN ts.ReceivedNotInvoiceTransCount = 1
                THEN ISNULL (ts.ReceivedNotInvoicedQuantity, 1) * frl.ReceivedNotInvoicedQuantity_FT
                     / ISNULL (NULLIF(frl.ReceivedNotInvoicedQuantity, 0), 1)
                ELSE 0 END                                                                                           AS ReceivedNotInvoicedQuantity_FT




         , CASE WHEN ts.ReceivedNotInvoiceTransCount = 1
                THEN ISNULL (ts.ReceivedNotInvoicedQuantity, 1) * frl.ReceivedNotInvoicedQuantity_SQIN
                     / ISNULL (NULLIF(frl.ReceivedNotInvoicedQuantity, 0), 1)
                ELSE 0 END                                                                                           AS ReceivedNotInvoicedQuantity_SQIN
         , ts.ReceivedNotInvoiceTransCount                                                                           AS ReceivedNotInvoiceTransCount
         , CASE WHEN ts.ReceivedNotInvoiceTransCount = 1
                THEN DATEDIFF (
                         d
                       , NULLIF(CASE WHEN dis.InventoryTransStatusID IN ( 1, 2 ) THEN it.datestatus ELSE NULL END, '1/1/1900')
                       , GETDATE ())
                ELSE NULL END                                                                                        AS ReceivedNotInvoicedDays
         , ISNULL (ts.RecID_IT, 0)                                                                                   AS _RecID2
         , frl._RecID                                                                                                AS _RecID1
         , 1                                                                                                         AS _SourceID
         ,  CURRENT_TIMESTAMP  AS  _CreatedDate
         , CURRENT_TIMESTAMP AS _ModifiedDate

 FROM {{ ref('productreceiptline_f') }}          frl
      LEFT JOIN productreceiptlinetrans_facttransadj4                         ts
        ON frl.ProductReceiptLineKey      = ts.ProductReceiptLineKey
      LEFT JOIN {{ ref('legalentity_d') }}                 le
        ON le.LegalEntityID               = ts.LegalEntityID
      LEFT JOIN {{ ref('inventory_trans_status_d') }}        dis
        ON dis.InventoryTransStatusTypeID = 2
       AND dis.InventoryTransStatusID     = ts.StatusReceipt
      LEFT JOIN {{ ref('tag_d') }}                         dt
        ON dt.LegalEntityID               = ts.LegalEntityID
       AND dt.TagID                       = ts.TagID
       AND dt.ItemID                      = ts.ItemID
      LEFT JOIN {{ ref('inventtrans') }}                 it
        ON it.recid                      = ts.RecID_IT
      LEFT JOIN {{ ref('purchaseorderlinetrans_f') }} polt
        ON polt._RecID2                   = ts.RecID_IT
       AND polt._SourceID                 = 1
      LEFT JOIN {{ ref('purchaseorderline_f') }}      polf
        ON polf._RecID                    = ts.RecID_PL
       AND polf._SourceID                 = 1
      LEFT JOIN {{ ref('exchangerate_f') }}           ex
        ON ex.ExchangeDateKey             = frl.ReceiptDateKey
       AND ex.FromCurrencyID              = le.AccountingCurrencyID
       AND ex.ToCurrencyID                = ts.CurrencyID
       AND ex.ExchangeRateType            = le.TransExchangeRateType
      LEFT JOIN productreceiptlinetrans_factordertrans                   ot
        ON ot.ProductReceiptLineKey       = frl.ProductReceiptLineKey
       AND ot.OrderTransRank              = 1;
