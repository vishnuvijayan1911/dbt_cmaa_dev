{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/purchaseinvoicelinetrans_fact/purchaseinvoicelinetrans_fact.py
-- Root method: PurchaseinvoicelinetransFact.purchaseinvoicelinetrans_factdetail [PurchaseInvoiceLineTrans_FactDetail]
-- Inlined methods: PurchaseinvoicelinetransFact.purchaseinvoicelinetrans_factstage [PurchaseInvoiceLineTrans_FactStage], PurchaseinvoicelinetransFact.purchaseinvoicelinetrans_factratio [PurchaseInvoiceLineTrans_FactRatio], PurchaseinvoicelinetransFact.purchaseinvoicelinetrans_facttrans [PurchaseInvoiceLineTrans_FactTrans], PurchaseinvoicelinetransFact.purchaseinvoicelinetrans_factadj [PurchaseInvoiceLineTrans_FactAdj], PurchaseinvoicelinetransFact.purchaseinvoicelinetrans_factadj1 [PurchaseInvoiceLineTrans_FactAdj1], PurchaseinvoicelinetransFact.purchaseinvoicelinetrans_factadj2 [PurchaseInvoiceLineTrans_FactAdj2], PurchaseinvoicelinetransFact.purchaseinvoicelinetrans_factadj3 [PurchaseInvoiceLineTrans_FactAdj3], PurchaseinvoicelinetransFact.purchaseinvoicelinetrans_factadj4 [PurchaseInvoiceLineTrans_FactAdj4], PurchaseinvoicelinetransFact.purchaseinvoicelinetrans_factinvoicetrans [PurchaseInvoiceLineTrans_FactInvoiceTrans], PurchaseinvoicelinetransFact.purchaseinvoicelinetrans_factinvoicetrans2 [PurchaseInvoiceLineTrans_FactInvoiceTrans2]
-- external_table_name: PurchaseInvoiceLineTrans_FactDetail
-- schema_name: temp

WITH
purchaseinvoicelinetrans_factstage AS (
    SELECT it.recid                   AS RecID_IT
             , MAX (vit.recid)            AS RecID_VIT
             , MAX (vit.inventqty)         AS InventoryQty_VIT
             , MAX (vit.itemid)            AS ItemID
             , MAX (it.inventdimid)        AS InventDimID
             , MAX (ito.dataareaid)       AS DataAreaID
             , MAX (vit.qty)               AS Qty_VIT
             , MAX (it.qty)                AS Qty_IT
             , MAX (it.packingslipid)      AS ProductReceiptID
             , MAX (vit.modifieddatetime) AS _SourceDate
          FROM {{ ref('vendinvoicejour') }}        vij
          LEFT JOIN {{ ref('vendinvoicetrans') }}  vit
            ON vit.dataareaid         = vij.dataareaid
           AND vit.purchid             = vij.purchid
           AND vit.invoiceid           = vij.invoiceid
           AND vit.invoicedate         = vij.invoicedate
           AND vit.numbersequencegroup = vij.numbersequencegroup
           AND vit.internalinvoiceid   = vij.internalinvoiceid
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid         = vit.dataareaid
           AND ito.inventtransid       = vit.inventtransid
           AND ito.itemid              = vit.itemid
           AND ito.referencecategory   = 3
         INNER JOIN {{ ref('inventtrans') }}       it
            ON it.dataareaid          = ito.dataareaid
           AND it.inventtransorigin    = ito.recid
           AND it.itemid               = ito.itemid
           AND it.invoiceid            = vit.invoiceid
         GROUP BY it.recid;
),
purchaseinvoicelinetrans_factratio AS (
    SELECT ts.RecID_IT                                                                                      AS RecID_IT
             , id.inventbatchid                                                                                 AS TagID
             , CASE WHEN SUM (ts.Qty_IT) OVER (PARTITION BY ts.RecID_VIT) = 0
                    THEN 1 / CAST(ISNULL (NULLIF(COUNT (1) OVER (PARTITION BY ts.RecID_VIT), 0), 1) AS FLOAT)
                    ELSE
                    CAST(ts.Qty_IT AS FLOAT)
                    / CAST(ISNULL (NULLIF(SUM (ts.Qty_IT) OVER (PARTITION BY ts.RecID_VIT), 0), 1) AS FLOAT)END AS PercentOfTotal

          FROM purchaseinvoicelinetrans_factstage             ts
         INNER JOIN {{ ref('inventdim') }} id
            ON id.dataareaid = ts.DataAreaID
           AND id.inventdimid = ts.InventDimID;
),
purchaseinvoicelinetrans_facttrans AS (
    SELECT fcl.PurchaseInvoiceLineKey                                                         AS PurchaseInvoiceLineKey
             , fcl.InvoiceDateKey                                                                 AS InvoiceDateKey
             , ts.DataAreaID                                                                      AS DataAreaID
             , ts.ItemID                                                                          AS ItemID
             , tr.TagID                                                                           AS TagID
             , CAST(ts.Qty_IT * ts.Qty_VIT / ISNULL (NULLIF(ts.InventoryQty_VIT, 0), 1) AS FLOAT) AS InvoiceQuantity_PurchUOM
             , ts.Qty_IT                                                                          AS InvoiceQuantity
             , CAST(fcl.BaseAmount * tr.PercentOfTotal AS MONEY)                                  AS BaseAmount
             , CAST(fcl.BaseAmount_TransCur * tr.PercentOfTotal AS MONEY)                         AS BaseAmount_TransCur
             , CAST(fcl.IncludedCharge * tr.PercentOfTotal AS MONEY)                              AS IncludedCharge
             , CAST(fcl.IncludedCharge_TransCur * tr.PercentOfTotal AS MONEY)                     AS IncludedCharge_TransCur
             , CAST(fcl.AdditionalCharge * tr.PercentOfTotal AS MONEY)                            AS AdditionalCharge
             , CAST(fcl.AdditionalCharge_TransCur * tr.PercentOfTotal AS MONEY)                   AS AdditionalCharge_TransCur
             , CAST(fcl.NonBillableCharge * tr.PercentOfTotal AS MONEY)                           AS NonBillableCharge
             , CAST(fcl.NonBillableCharge_TransCur * tr.PercentOfTotal AS MONEY)                  AS NonBillableCharge_TransCur
             , CAST(fcl.DiscountAmount * tr.PercentOfTotal AS MONEY)                              AS DiscountAmount
             , CAST(fcl.DiscountAmount_TransCur * tr.PercentOfTotal AS MONEY)                     AS DiscountAmount_TransCur
             , CAST(fcl.InvoiceTotalAmount * tr.PercentOfTotal AS MONEY)                          AS InvoiceTotalAmount
             , CAST(fcl.InvoiceTotalAmount_TransCur * tr.PercentOfTotal AS MONEY)                 AS InvoiceTotalAmount_TransCur
             , CAST(fcl.NetAmount * tr.PercentOfTotal AS MONEY)                                   AS NetAmount
             , CAST(fcl.NetAmount_TransCur * tr.PercentOfTotal AS MONEY)                          AS NetAmount_TransCur
             , CAST(fcl.InvoicePurchaseAmount * tr.PercentOfTotal AS MONEY)                       AS InvoicePurchaseAmount
             , CAST(fcl.InvoicePurchaseAmount_TransCur * tr.PercentOfTotal AS MONEY)              AS InvoicePurchaseAmount_TransCur
             , CAST(fcl.TaxAmount * tr.PercentOfTotal AS MONEY)                                   AS TaxAmount
             , CAST(fcl.TaxAmount_TransCur * tr.PercentOfTotal AS MONEY)                          AS TaxAmount_TransCur
             , CAST(fcl.VendorCharge * tr.PercentOfTotal AS MONEY)                                AS VendorCharge
             , CAST(fcl.VendorCharge_TransCur * tr.PercentOfTotal AS MONEY)                       AS VendorCharge_TransCur
             , fcl.BaseUnitPrice                                                                  AS BaseUnitPrice
             , fcl.TotalUnitPrice                                                                 AS TotalUnitPrice
             , fcl.PriceUnit                                                              AS PriceUnit
             , tr.RecID_IT                                                                        AS RecID_IT
             , ts.RecID_VIT
             , ts.ProductReceiptID                                                                AS ProductReceiptID
             , ts._SourceDate
             , CASE WHEN ROW_NUMBER () OVER (PARTITION BY fcl.PurchaseInvoiceLineKey
    ORDER BY ISNULL (tr.RecID_IT, 0)) = 1
                    THEN 1
                    ELSE 0 END                                                                    AS IsProrateAdj
          FROM silver.cma_PurchaseInvoiceLine_Fact fcl
         INNER JOIN purchaseinvoicelinetrans_factstage                  ts
            ON ts.RecID_VIT = fcl._RecID2
         INNER JOIN purchaseinvoicelinetrans_factratio                  tr
            ON tr.RecID_IT  = ts.RecID_IT
         WHERE fcl._SourceID = 1;
),
purchaseinvoicelinetrans_factadj AS (
    SELECT t.PurchaseInvoiceLineKey
             , t.RecID_IT
             , t.RecID_VIT
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.BaseAmount - SUM (t.BaseAmount) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS BaseAmountAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.BaseAmount_TransCur
                              - SUM (t.BaseAmount_TransCur) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS BaseAmount_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.DiscountAmount - SUM (t.DiscountAmount) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS DiscountAmountAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.DiscountAmount_TransCur
                              - SUM (t.DiscountAmount_TransCur) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS DiscountAmount_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.IncludedCharge - SUM (t.IncludedCharge) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS IncludedChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.IncludedCharge_TransCur
                              - SUM (t.IncludedCharge_TransCur) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS IncludedCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.AdditionalCharge - SUM (t.AdditionalCharge) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS AdditionalChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.AdditionalCharge_TransCur
                              - SUM (t.AdditionalCharge_TransCur) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS AdditionalCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.VendorCharge - SUM (t.VendorCharge) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS VendorChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.VendorCharge_TransCur
                              - SUM (t.VendorCharge_TransCur) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS VendorCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NonBillableCharge
                              - SUM (t.NonBillableCharge) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS NonBillableChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NonBillableCharge_TransCur
                              - SUM (t.NonBillableCharge_TransCur) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS NonBillableCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.InvoiceTotalAmount
                              - SUM (t.InvoiceTotalAmount) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS InvoiceTotalAmountAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.InvoiceTotalAmount_TransCur
                              - SUM (t.InvoiceTotalAmount_TransCur) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS InvoiceTotalAmount_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NetAmount - SUM (t.NetAmount) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS NetAmountAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NetAmount_TransCur
                              - SUM (t.NetAmount_TransCur) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS NetAmount_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.InvoicePurchaseAmount
                              - SUM (t.InvoicePurchaseAmount) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS InvoicePurchaseAmountAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.InvoicePurchaseAmount_TransCur
                              - SUM (t.InvoicePurchaseAmount_TransCur) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS InvoicePurchaseAmount_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TaxAmount - SUM (t.TaxAmount) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS TaxAmountAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TaxAmount_TransCur
                              - SUM (t.TaxAmount_TransCur) OVER (PARTITION BY t.PurchaseInvoiceLineKey)
                         ELSE 0 END AS MONEY) AS TaxAmount_TransCurAdj

          FROM purchaseinvoicelinetrans_facttrans                           t
         INNER JOIN silver.cma_PurchaseInvoiceLine_Fact fcl
            ON fcl.PurchaseInvoiceLineKey = t.PurchaseInvoiceLineKey
),
purchaseinvoicelinetrans_factadj1 AS (
    SELECT PurchaseInvoiceLineKey
             , RecID_IT
             , RecID_VIT
             , BaseAmountAdj
             , BaseAmount_TransCurAdj
             , DiscountAmountAdj
             , DiscountAmount_TransCurAdj
             , IncludedChargeAdj
             , IncludedCharge_TransCurAdj
             , AdditionalChargeAdj
             , AdditionalCharge_TransCurAdj
             , VendorChargeAdj
             , VendorCharge_TransCurAdj
             , NonBillableChargeAdj
             , NonBillableCharge_TransCurAdj
             , InvoiceTotalAmountAdj
             , InvoiceTotalAmount_TransCurAdj
             , NetAmountAdj
             , NetAmount_TransCurAdj
             , InvoicePurchaseAmountAdj
             , InvoicePurchaseAmount_TransCurAdj
             , TaxAmountAdj
             , TaxAmount_TransCurAdj

          FROM purchaseinvoicelinetrans_factadj
         WHERE BaseAmountAdj                     <> 0
            OR BaseAmount_TransCurAdj            <> 0
            OR DiscountAmountAdj                 <> 0
            OR DiscountAmount_TransCurAdj        <> 0
            OR IncludedChargeAdj                 <> 0
            OR IncludedCharge_TransCurAdj        <> 0
            OR AdditionalChargeAdj               <> 0
            OR AdditionalCharge_TransCurAdj      <> 0
            OR VendorChargeAdj                   <> 0
            OR VendorCharge_TransCurAdj          <> 0
            OR NonBillableChargeAdj              <> 0
            OR NonBillableCharge_TransCurAdj     <> 0
            OR InvoiceTotalAmountAdj             <> 0
            OR InvoiceTotalAmount_TransCurAdj    <> 0
            OR NetAmountAdj                      <> 0
            OR NetAmount_TransCurAdj             <> 0
            OR InvoicePurchaseAmountAdj          <> 0
            OR InvoicePurchaseAmount_TransCurAdj <> 0
            OR TaxAmountAdj                      <> 0
            OR TaxAmount_TransCurAdj             <> 0;
),
purchaseinvoicelinetrans_factadj2 AS (
    SELECT t.PurchaseInvoiceLineKey
          ,t.InvoiceDateKey
          ,t.DataAreaID
          ,t.ItemID
          ,t.TagID
          ,t.InvoiceQuantity_PurchUOM
          ,t.InvoiceQuantity
          ,t.BaseAmount + ta.BaseAmountAdj                                              AS BaseAmount
          ,t.BaseAmount_TransCur + ta.BaseAmount_TransCurAdj                            AS BaseAmount_TransCur
          ,t.IncludedCharge + ta.IncludedChargeAdj                                      AS IncludedCharge
          ,t.IncludedCharge_TransCur + ta.IncludedCharge_TransCurAdj                    AS IncludedCharge_TransCur
          ,t.AdditionalCharge + ta.AdditionalChargeAdj                                  AS AdditionalCharge
          ,t.AdditionalCharge_TransCur + ta.AdditionalCharge_TransCurAdj                AS AdditionalCharge_TransCur
          ,t.NonBillableCharge + ta.NonBillableChargeAdj                                AS NonBillableCharge
          ,t.NonBillableCharge_TransCur + ta.NonBillableCharge_TransCurAdj              AS NonBillableCharge_TransCur
          ,t.DiscountAmount + ta.DiscountAmountAdj                                      AS DiscountAmount
          ,t.DiscountAmount_TransCur + ta.DiscountAmount_TransCurAdj                    AS DiscountAmount_TransCur
          ,t.InvoiceTotalAmount + ta.InvoiceTotalAmountAdj                              AS InvoiceTotalAmount
          ,t.InvoiceTotalAmount_TransCur + ta.InvoiceTotalAmount_TransCurAdj            AS InvoiceTotalAmount_TransCur
          ,t.NetAmount + ta.NetAmountAdj                                                AS NetAmount
          ,t.NetAmount_TransCur + ta.NetAmount_TransCurAdj                              AS NetAmount_TransCur
          ,t.InvoicePurchaseAmount + ta.InvoicePurchaseAmountAdj                        AS InvoicePurchaseAmount
          ,t.InvoicePurchaseAmount_TransCur + ta.InvoicePurchaseAmount_TransCurAdj      AS InvoicePurchaseAmount_TransCur
          ,t.TaxAmount + ta.TaxAmountAdj                                                AS TaxAmount
          ,t.TaxAmount_TransCur + ta.TaxAmount_TransCurAdj                              AS TaxAmount_TransCur
          ,t.VendorCharge + ta.VendorChargeAdj                                          AS VendorCharge
          ,t.VendorCharge_TransCur + ta.VendorCharge_TransCurAdj                        AS VendorCharge_TransCur
          ,t.BaseUnitPrice
          ,t.TotalUnitPrice
          ,t.PriceUnit
          ,t.RecID_IT
          ,t.ProductReceiptID
          ,t._SourceDate
          ,t.IsProrateAdj
       FROM purchaseinvoicelinetrans_facttrans         t
       INNER JOIN purchaseinvoicelinetrans_factadj1 ta
          ON ta.RecID_VIT = t.RecID_VIT
         AND ta.RecID_IT  = t.RecID_IT
),
purchaseinvoicelinetrans_factadj3 AS (
    SELECT t.PurchaseInvoiceLineKey
            ,t.InvoiceDateKey
            ,t.DataAreaID
            ,t.ItemID
            ,t.TagID
            ,t.InvoiceQuantity_PurchUOM
            ,t.InvoiceQuantity
            ,t.BaseAmount
            ,t.BaseAmount_TransCur
            ,t.IncludedCharge
            ,t.IncludedCharge_TransCur
            ,t.AdditionalCharge
            ,t.AdditionalCharge_TransCur
            ,t.NonBillableCharge
            ,t.NonBillableCharge_TransCur
            ,t.DiscountAmount
            ,t.DiscountAmount_TransCur
            ,t.InvoiceTotalAmount
            ,t.InvoiceTotalAmount_TransCur
            ,t.NetAmount
            ,t.NetAmount_TransCur
            ,t.InvoicePurchaseAmount
            ,t.InvoicePurchaseAmount_TransCur
            ,t.TaxAmount
            ,t.TaxAmount_TransCur
            ,t.VendorCharge
            ,t.VendorCharge_TransCur
            ,t.BaseUnitPrice
            ,t.TotalUnitPrice
            ,t.PriceUnit
            ,t.RecID_IT
            ,t.ProductReceiptID
            ,t._SourceDate
            ,t.IsProrateAdj
         FROM purchaseinvoicelinetrans_facttrans         t
         LEFT JOIN purchaseinvoicelinetrans_factadj1 ta
            ON ta.RecID_VIT = t.RecID_VIT
           AND ta.RecID_IT  = t.RecID_IT
    	 WHERE (ta.RecID_IT IS NULL) or (ta.RecID_VIT IS NULL)
),
purchaseinvoicelinetrans_factadj4 AS (
    SELECT * FROM purchaseinvoicelinetrans_factadj2
    UNION ALL
    SELECT * FROM purchaseinvoicelinetrans_factadj3
),
purchaseinvoicelinetrans_factinvoicetrans AS (
    SELECT frl.PurchaseInvoiceLineKey
             , polt.PurchaseOrderLineTransKey
             , ROW_NUMBER () OVER (PARTITION BY frl.PurchaseInvoiceLineKey
    ORDER BY polt._RecID2) AS OrderTransRank

          FROM silver.cma_PurchaseInvoiceLine_Fact         frl

         INNER JOIN silver.cma_PurchaseOrderLineTrans_Fact polt
            ON polt.PurchaseOrderLineKey = frl.PurchaseOrderLineKey
         WHERE frl.PurchaseOrderLineKey <> -1;
),
purchaseinvoicelinetrans_factinvoicetrans2 AS (
    SELECT frl.PurchaseInvoiceLineKey
             , prlt.ProductReceiptLineTransKey
             , prlt.PurchaseOrderLineTransKey
             , ROW_NUMBER () OVER (PARTITION BY frl.PurchaseInvoiceLineKey
    ORDER BY prlt._RecID2) AS OrderTransRank

          FROM silver.cma_PurchaseInvoiceLine_Fact          frl

         INNER JOIN silver.cma_ProductReceiptLine_Fact      prl
            ON prl.PurchaseOrderLineKey   = frl.PurchaseOrderLineKey
         INNER JOIN silver.cma_ProductReceiptLineTrans_Fact prlt
            ON prlt.ProductReceiptLineKey = prl.ProductReceiptLineKey
         WHERE frl.PurchaseOrderLineKey <> -1;
)
SELECT ROW_NUMBER() OVER (ORDER BY fcl._RecID1, fcl._RecID2, tt.RecID_IT) AS PurchaseInvoiceLineTransKey
         , fcl.PurchaseInvoiceLineKey                                                                                  AS PurchaseInvoiceLineKey
         , COALESCE (prlt.ProductReceiptLineTransKey, it2.ProductReceiptLineTransKey, -1)                              AS ProductReceiptLineTransKey
         , COALESCE (prlt.PurchaseOrderLineTransKey, it1.PurchaseOrderLineTransKey, it2.PurchaseOrderLineTransKey, -1) AS PurchaseOrderLineTransKey
         , dt.TagKey                                                                                                   AS TagKey
         , ISNULL (tt.AdditionalCharge, fcl.AdditionalCharge)                                                          AS AdditionalCharge
         , ISNULL (tt.AdditionalCharge_TransCur, fcl.AdditionalCharge_TransCur)                                        AS AdditionalCharge_TransCur
         , ISNULL (tt.BaseAmount, fcl.BaseAmount)                                                                      AS BaseAmount
         , ISNULL (tt.BaseAmount_TransCur, fcl.BaseAmount_TransCur)                                                    AS BaseAmount_TransCur
         , fcl.BaseUnitPrice                                                                                           AS BaseUnitPrice
         , fcl.BaseUnitPrice_TransCur                                                                                  AS BaseUnitPrice_TransCur
         , ISNULL (tt.DiscountAmount, fcl.DiscountAmount)                                                              AS DiscountAmount
         , ISNULL (tt.DiscountAmount_TransCur, fcl.DiscountAmount_TransCur)                                            AS DiscountAmount_TransCur
         , ISNULL (tt.IncludedCharge, fcl.IncludedCharge)                                                              AS IncludedCharge
         , ISNULL (tt.IncludedCharge_TransCur, fcl.IncludedCharge_TransCur)                                            AS IncludedCharge_TransCur
         , ISNULL (tt.InvoiceTotalAmount, fcl.InvoiceTotalAmount)                                                      AS InvoiceTotalAmount
         , ISNULL (tt.InvoiceTotalAmount_TransCur, fcl.InvoiceTotalAmount_TransCur)                                    AS InvoiceTotalAmount_TransCur
         , ISNULL (tt.InvoiceQuantity_PurchUOM, fcl.InvoiceQuantity_PurchUOM)                                          AS InvoiceQuantity_PurchUOM
         , ISNULL (tt.InvoiceQuantity, fcl.InvoiceQuantity)                                                            AS InvoiceQuantity
         , ISNULL (tt.InvoiceQuantity, 1) * fcl.InvoiceQuantity_LB / ISNULL (NULLIF(fcl.InvoiceQuantity, 0), 1)        AS InvoiceQuantity_LB
         , ISNULL (tt.InvoiceQuantity, 1) * fcl.InvoiceQuantity_PC / ISNULL (NULLIF(fcl.InvoiceQuantity, 0), 1)        AS InvoiceQuantity_PC

         , ISNULL (tt.InvoiceQuantity, 1) * fcl.InvoiceQuantity_FT / ISNULL (NULLIF(fcl.InvoiceQuantity, 0), 1)        AS InvoiceQuantity_FT

         , ISNULL (tt.InvoiceQuantity, 1) * fcl.InvoiceQuantity_SQIN / ISNULL (NULLIF(fcl.InvoiceQuantity, 0), 1)      AS InvoiceQuantity_SQIN
         , ISNULL (tt.NetAmount, fcl.NetAmount)                                                                        AS NetAmount
         , ISNULL (tt.NetAmount_TransCur, fcl.NetAmount_TransCur)                                                      AS NetAmount_TransCur
         , ISNULL (tt.NonBillableCharge, fcl.NonBillableCharge)                                                        AS NonBillableCharge
         , ISNULL (tt.NonBillableCharge_TransCur, fcl.NonBillableCharge_TransCur)                                      AS NonBillableCharge_TransCur
         , fcl.PriceUnit                                                                                               AS PriceUnit
         , ISNULL (tt.TaxAmount, fcl.TaxAmount)                                                                        AS TaxAmount
         , ISNULL (tt.TaxAmount_TransCur, fcl.TaxAmount_TransCur)                                                      AS TaxAmount_TransCur
         , ISNULL (tt.InvoicePurchaseAmount, fcl.InvoicePurchaseAmount)                                                AS InvoicePurchaseAmount
         , ISNULL (tt.InvoicePurchaseAmount_TransCur, fcl.InvoicePurchaseAmount_TransCur)                              AS InvoicePurchaseAmount_TransCur
         , fcl.TotalUnitPrice                                                                                          AS TotalUnitPrice
         , fcl.TotalUnitPrice_TransCur                                                                                 AS TotalUnitPrice_TransCur
         , ISNULL (tt.VendorCharge, fcl.VendorCharge)                                                                  AS VendorCharge
         , ISNULL (tt.VendorCharge_TransCur, fcl.VendorCharge_TransCur)                                                AS VendorCharge_TransCur
         , tt.ProductReceiptID                                                                                         AS ProductReceiptID
         , tt._SourceDate
         , ISNULL (tt.RecID_IT, 0)                                                                                     AS _RecID3
         , fcl._RecID2                                                                                                 AS _RecID2
         , fcl._RecID1                                                                                                 AS _RecID1
         , 1                                                                                                           AS _SourceID
         ,  CURRENT_TIMESTAMP  AS  _CreatedDate
         , CURRENT_TIMESTAMP AS _ModifiedDate

      FROM silver.cma_PurchaseInvoiceLine_Fact          fcl
      LEFT JOIN purchaseinvoicelinetrans_factadj4                          tt
        ON tt.PurchaseInvoiceLineKey  = fcl.PurchaseInvoiceLineKey
      LEFT JOIN silver.cma_Tag                          dt
        ON dt.LegalEntityID           = tt.DataAreaID
       AND dt.TagID                   = tt.TagID
       AND dt.ItemID                  = tt.ItemID
      LEFT JOIN silver.cma_ProductReceiptLineTrans_Fact prlt
        ON prlt._RecID2               = tt.RecID_IT
       AND prlt._SourceID             = 1
      LEFT JOIN purchaseinvoicelinetrans_factinvoicetrans                    it1
        ON it1.PurchaseInvoiceLineKey = fcl.PurchaseInvoiceLineKey
       AND it1.OrderTransRank         = 1
      LEFT JOIN purchaseinvoicelinetrans_factinvoicetrans2                   it2
        ON it2.PurchaseInvoiceLineKey = fcl.PurchaseInvoiceLineKey
       AND it2.OrderTransRank         = 1;
