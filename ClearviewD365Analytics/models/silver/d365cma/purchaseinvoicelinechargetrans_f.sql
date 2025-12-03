{{ config(materialized='table', tags=['silver'], alias='purchaseinvoicelinechargetrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseinvoicelinechargetrans_f/purchaseinvoicelinechargetrans_f.py
-- Root method: PurchaseinvoicelinechargetransFact.purchaseinvoicelinechargetrans_factdetail [PurchaseInvoiceLineChargeTrans_FactDetail]
-- Inlined methods: PurchaseinvoicelinechargetransFact.purchaseinvoicelinechargetrans_factpilkeys [PurchaseInvoiceLineChargeTrans_FactPILKeys], PurchaseinvoicelinechargetransFact.purchaseinvoicelinechargetrans_factstage [PurchaseInvoiceLineChargeTrans_FactStage], PurchaseinvoicelinechargetransFact.purchaseinvoicelinechargetrans_factpilratio [PurchaseInvoiceLineChargeTrans_FactPILRatio], PurchaseinvoicelinechargetransFact.purchaseinvoicelinechargetrans_facttrans [PurchaseInvoiceLineChargeTrans_FactTrans], PurchaseinvoicelinechargetransFact.purchaseinvoicelinechargetrans_factadj [PurchaseInvoiceLineChargeTrans_FactAdj], PurchaseinvoicelinechargetransFact.purchaseinvoicelinechargetrans_facttransadj [PurchaseInvoiceLineChargeTrans_FactTransAdj], PurchaseinvoicelinechargetransFact.purchaseinvoicelinechargetrans_facttrans2 [PurchaseInvoiceLineChargeTrans_FactTrans2], PurchaseinvoicelinechargetransFact.purchaseinvoicelinechargetrans_facttrans3 [PurchaseInvoiceLineChargeTrans_FactTrans3], PurchaseinvoicelinechargetransFact.purchaseinvoicelinechargetrans_facttrans4 [PurchaseInvoiceLineChargeTrans_FactTrans4]
-- external_table_name: PurchaseInvoiceLineChargeTrans_FactDetail
-- schema_name: temp

WITH
purchaseinvoicelinechargetrans_factpilkeys AS (
    SELECT t.RecID_IT
             , t.RecID_VIT
             , t.RecID_VIJ
             , t.InventoryQty_VIT
             , t.ItemID
             , t.Qty_VIT
             , t.Qty_IT
             , t.RecID_MT
             , t.TaxCurrencyID
             , t.LegalEntityID

          FROM (   SELECT it.recid             AS RecID_IT
                        , MAX(vit.recid)       AS RecID_VIT
                        , MAX(vij.recid)       AS RecID_VIJ
                        , MAX(vit.inventqty)   AS InventoryQty_VIT
                        , MAX(vit.itemid)      AS ItemID
                        , MAX(vit.qty)         AS Qty_VIT
                        , MAX(it.qty)          AS Qty_IT
                        , mt.recid             AS RecID_MT
                        , MAX(mt.currencycode) AS TaxCurrencyID
                        , MAX(mt.dataareaid)   AS LegalEntityID
                     FROM {{ ref('markuptrans') }}            mt
                    INNER JOIN {{ ref('markuptable') }}       mu
                       ON mu.dataareaid           = mt.dataareaid
                      AND mu.markupcode           = mt.markupcode
                      AND mu.moduletype           = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }}     sd
                       ON sd.fieldid              = 0
                      AND sd.tabid              = mt.transtableid
                      AND sd.name                 = 'VENDINVOICETRANS'
                    INNER JOIN {{ ref('vendinvoicetrans') }}  vit
                       ON vit.recid               = mt.transrecid
                    INNER JOIN {{ ref('vendinvoicejour') }}   vij
                       ON vij.dataareaid          = vit.dataareaid
                      AND vij.purchid             = vit.purchid
                      AND vij.invoiceid           = vit.invoiceid
                      AND vij.invoicedate         = vit.invoicedate
                      AND vij.numbersequencegroup = vit.numbersequencegroup
                      AND vij.internalinvoiceid   = vit.internalinvoiceid
                    INNER JOIN {{ ref('inventtransorigin') }} ito
                       ON ito.dataareaid          = vit.dataareaid
                      AND ito.inventtransid       = vit.inventtransid
                      AND ito.itemid              = vit.itemid
                      AND ito.referencecategory   = 3
                    INNER JOIN {{ ref('inventtrans') }}       it
                       ON it.dataareaid           = ito.dataareaid
                      AND it.inventtransorigin    = ito.recid
                      AND it.itemid               = ito.itemid
                      AND it.invoiceid            = vit.invoiceid
                    GROUP BY it.recid
                           , mt.recid
                   UNION
                   SELECT it.recid             AS RecID_IT
                        , MAX(vit.recid)       AS RecID_VIT
                        , MAX(vij.recid)       AS RecID_VIJ
                        , MAX(vit.inventqty)   AS InventoryQty_VIT
                        , MAX(vit.itemid)      AS ItemID
                        , MAX(vit.qty)         AS Qty_VIT
                        , MAX(it.qty)          AS Qty_IT
                        , mt.recid             AS RecID_MT
                        , MAX(mt.currencycode) AS TaxCurrencyID
                        , MAX(mt.dataareaid)   AS LegalEntityID
                     FROM {{ ref('markuptrans') }}            mt
                    INNER JOIN {{ ref('markuptable') }}       mu
                       ON mu.dataareaid           = mt.dataareaid
                      AND mu.markupcode           = mt.markupcode
                      AND mu.moduletype           = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }}     sd
                       ON sd.fieldid              = 0
                      AND sd.tabid              = mt.transtableid
                      AND sd.name                 = 'VendInvoiceJour'
                     LEFT JOIN {{ ref('vendinvoicejour') }}   vij
                       ON vij.recid               = mt.transrecid
                     LEFT JOIN {{ ref('vendinvoicetrans') }}  vit
                       ON vit.dataareaid          = vij.dataareaid
                      AND vit.purchid             = vij.purchid
                      AND vit.invoiceid           = vij.invoiceid
                      AND vit.invoicedate         = vij.invoicedate
                      AND vit.numbersequencegroup = vij.numbersequencegroup
                      AND vit.internalinvoiceid   = vij.internalinvoiceid
                    INNER JOIN {{ ref('inventtransorigin') }} ito
                       ON ito.dataareaid          = vit.dataareaid
                      AND ito.inventtransid       = vit.inventtransid
                      AND ito.itemid              = vit.itemid
                      AND ito.referencecategory   = 3
                    INNER JOIN {{ ref('inventtrans') }}       it
                       ON it.dataareaid           = ito.dataareaid
                      AND it.inventtransorigin    = ito.recid
                      AND it.itemid               = ito.itemid
                      AND it.invoiceid            = vit.invoiceid
                    GROUP BY it.recid
                           , mt.recid) AS t;
),
purchaseinvoicelinechargetrans_factstage AS (
    SELECT it.recid       AS RECID_IT
             , MAX(vit.recid) AS RECID_VIT
             , MAX(it.qty)    AS QTY_IT

          FROM {{ ref('vendinvoicetrans') }}       vit
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid        = vit.dataareaid
           AND ito.inventtransid     = vit.inventtransid
           AND ito.itemid            = vit.itemid
           AND ito.referencecategory = 3
         INNER JOIN {{ ref('inventtrans') }}       it
            ON it.dataareaid         = ito.dataareaid
           AND it.inventtransorigin  = ito.recid
           AND it.itemid             = ito.itemid
           AND it.invoiceid          = vit.invoiceid
         GROUP BY it.recid;
),
purchaseinvoicelinechargetrans_factpilratio AS (
    SELECT tk.RecID_IT
             , CASE WHEN SUM(tk.QTY_IT) OVER (PARTITION BY tk.RECID_VIT) = 0
                    THEN 1 / CAST(ISNULL(NULLIF(COUNT(1) OVER (PARTITION BY tk.RECID_VIT), 0), 1) AS FLOAT)
                    ELSE
                    CAST(tk.QTY_IT AS FLOAT)
                    / CAST(ISNULL(NULLIF(SUM(tk.QTY_IT) OVER (PARTITION BY tk.RECID_VIT), 0), 1) AS FLOAT)END AS PercentOfTotal

          FROM purchaseinvoicelinechargetrans_factstage tk;
),
purchaseinvoicelinechargetrans_facttrans AS (
    SELECT fc.PurchaseInvoiceLineChargeKey
             , CAST(fc.IncludedCharge_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)    AS IncludedCharge_TransCur
             , CAST(fc.IncludedCharge * ISNULL(tr.PercentOfTotal, 1) AS MONEY)             AS IncludedCharge
             , CAST(fc.AdditionalCharge_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)  AS AdditionalCharge_TransCur
             , CAST(fc.AdditionalCharge * ISNULL(tr.PercentOfTotal, 1) AS MONEY)           AS AdditionalCharge
             , CAST(fc.NonBillableCharge_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY) AS NonBillableCharge_TransCur
             , CAST(fc.NonBillableCharge * ISNULL(tr.PercentOfTotal, 1) AS MONEY)          AS NonBillableCharge
             , CAST(fc.TotalCharges_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)      AS TotalCharges_TransCur
             , CAST(fc.TotalCharges * ISNULL(tr.PercentOfTotal, 1) AS MONEY)               AS TotalCharges
             , CAST(fc.TaxAmount_TransCur * ISNULL(tr.PercentOfTotal, 1) AS MONEY)         AS TaxAmount_TransCur
             , CAST(fc.TaxAmount * ISNULL(tr.PercentOfTotal, 1) AS MONEY)                  AS TaxAmount
             , ISNULL(tk.RecID_IT, 0)                                                      AS _RecID3
             , CASE WHEN ROW_NUMBER() OVER (PARTITION BY fc.PurchaseInvoiceLineChargeKey
                                                ORDER BY ISNULL(tk.RecID_IT, 0)
                                                       , ISNULL(tk.RecID_MT, 0)) = 1
                    THEN 1
                    ELSE 0 END                                                             AS IsProrateAdj
             , tk.RecID_VIT
             , tk.RecID_MT

        FROM {{ ref('purchaseinvoicelinecharge_f') }} fc
         INNER JOIN purchaseinvoicelinechargetrans_factpilkeys                      tk
            ON tk.RecID_MT  = fc._RecID1
           AND tk.RecID_VIT = fc._RECID2
         INNER JOIN purchaseinvoicelinechargetrans_factpilratio                    tr
            ON tr.RecID_IT  = tk.RecID_IT;
),
purchaseinvoicelinechargetrans_factadj AS (
    SELECT t.PurchaseInvoiceLineChargeKey
             , t.RecID_MT
             , t._RecID3
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.IncludedCharge_TransCur
                              - SUM(t.IncludedCharge_TransCur) OVER (PARTITION BY t.PurchaseInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS IncludedCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.IncludedCharge - SUM(t.IncludedCharge) OVER (PARTITION BY t.PurchaseInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS IncludedChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.AdditionalCharge_TransCur
                              - SUM(t.AdditionalCharge_TransCur) OVER (PARTITION BY t.PurchaseInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS AdditionalCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.AdditionalCharge
                              - SUM(t.AdditionalCharge) OVER (PARTITION BY t.PurchaseInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS AdditionalChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NonBillableCharge
                              - SUM(t.NonBillableCharge) OVER (PARTITION BY t.PurchaseInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS NonBillableChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NonBillableCharge_TransCur
                              - SUM(t.NonBillableCharge_TransCur) OVER (PARTITION BY t.PurchaseInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS NonBillableCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TotalCharges_TransCur
                              - SUM(t.TotalCharges_TransCur) OVER (PARTITION BY t.PurchaseInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS TotalCharges_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TotalCharges - SUM(t.TotalCharges) OVER (PARTITION BY t.PurchaseInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS TotalChargesAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TaxAmount_TransCur
                              - SUM(t.TaxAmount_TransCur) OVER (PARTITION BY t.PurchaseInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS TaxAmount_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TaxAmount - SUM(t.TaxAmount) OVER (PARTITION BY t.PurchaseInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS TaxAmountAdj

          FROM purchaseinvoicelinechargetrans_facttrans                                  t
         INNER JOIN {{ ref('purchaseinvoicelinecharge_f') }} fcl
            ON fcl.PurchaseInvoiceLineChargeKey = t.PurchaseInvoiceLineChargeKey
),
purchaseinvoicelinechargetrans_facttransadj AS (
    SELECT PurchaseInvoiceLineChargeKey
             , _RecID3
             , RecID_MT
             , IncludedCharge_TransCurAdj
             , IncludedChargeAdj
             , AdditionalCharge_TransCurAdj
             , AdditionalChargeAdj
             , NonBillableCharge_TransCurAdj
             , NonBillableChargeAdj
             , TotalCharges_TransCurAdj
             , TotalChargesAdj
             , TaxAmount_TransCurAdj
             , TaxAmountAdj

          FROM purchaseinvoicelinechargetrans_factadj
         WHERE IncludedCharge_TransCurAdj    <> 0
            OR IncludedChargeAdj             <> 0
            OR AdditionalCharge_TransCurAdj  <> 0
            OR AdditionalChargeAdj           <> 0
            OR NonBillableCharge_TransCurAdj <> 0
            OR NonBillableChargeAdj          <> 0
            OR TotalCharges_TransCurAdj      <> 0
            OR TotalChargesAdj               <> 0
            OR TaxAmount_TransCurAdj         <> 0
            OR TaxAmountAdj                  <> 0;
),
purchaseinvoicelinechargetrans_facttrans2 AS (
    SELECT 
          t.PurchaseInvoiceLineChargeKey,
           t.IncludedCharge_TransCur + ta.IncludedCharge_TransCurAdj                                            AS IncludedCharge_TransCur,
           t.IncludedCharge + ta.IncludedChargeAdj                                                              AS IncludedCharge,
           t.AdditionalCharge_TransCur + ta.AdditionalCharge_TransCurAdj                                        AS AdditionalCharge_TransCur,
           t.AdditionalCharge + ta.AdditionalChargeAdj                                                          AS AdditionalCharge,
           t.NonBillableCharge_TransCur + ta.NonBillableCharge_TransCurAdj                                      AS NonBillableCharge_TransCur,
           t.NonBillableCharge + ta.NonBillableChargeAdj                                                        AS NonBillableCharge,
           t.TotalCharges_TransCur + ta.TotalCharges_TransCurAdj                                                AS TotalCharges_TransCur,
           t.TotalCharges + ta.TotalChargesAdj                                                                  AS TotalCharges,
           t.TaxAmount_TransCur + ta.TaxAmount_TransCurAdj                                                      AS TaxAmount_TransCur,
           t.TaxAmount + ta.TaxAmountAdj                                                                        AS TaxAmount,
           t._RecID3,
           t.IsProrateAdj,
           t.RecID_VIT,
           t.RecID_MT


          FROM purchaseinvoicelinechargetrans_facttrans         t
         INNER JOIN purchaseinvoicelinechargetrans_facttransadj ta
            ON ta.RecID_MT = t.RecID_MT
           AND ta._RecID3  = t._RecID3;
),
purchaseinvoicelinechargetrans_facttrans3 AS (
    SELECT 

                t.PurchaseInvoiceLineChargeKey,
                t.IncludedCharge_TransCur,
                t.IncludedCharge,
                t.AdditionalCharge_TransCur,
                t.AdditionalCharge,
                t.NonBillableCharge_TransCur,
                t.NonBillableCharge,
                t.TotalCharges_TransCur,
                t.TotalCharges,
                t.TaxAmount_TransCur,
                t.TaxAmount,
                t._RecID3,
                t.IsProrateAdj,
                t.RecID_VIT,
                t.RecID_MT

          FROM purchaseinvoicelinechargetrans_facttrans         t
         LEFT JOIN purchaseinvoicelinechargetrans_facttransadj ta
            ON ta.RecID_MT = t.RecID_MT
           AND ta._RecID3  = t._RecID3
           WHERE ta.RecID_MT IS NULL
),
purchaseinvoicelinechargetrans_facttrans4 AS (
    SELECT * FROM purchaseinvoicelinechargetrans_facttrans2
     UNION ALL
      SELECT * FROM purchaseinvoicelinechargetrans_facttrans3
)
SELECT DISTINCT
           {{ dbt_utils.generate_surrogate_key(['fc._RecID1']) }} AS PurchaseInvoiceLineChargeTransKey
         , ISNULL(fc.PurchaseInvoiceLineChargeKey, -1)                          AS PurchaseInvoiceLineChargeKey
         , ISNULL(pilt.PurchaseInvoiceLineTransKey, -1)                         AS PurchaseInvoiceLineTransKey
         , ISNULL(tt.IncludedCharge_TransCur, fc.IncludedCharge_TransCur)       AS IncludedCharge_TransCur
         , ISNULL(tt.IncludedCharge, fc.IncludedCharge)                         AS IncludedCharge
         , ISNULL(tt.AdditionalCharge_TransCur, fc.AdditionalCharge_TransCur)   AS AdditionalCharge_TransCur
         , ISNULL(tt.AdditionalCharge, fc.AdditionalCharge)                     AS AdditionalCharge
         , ISNULL(tt.NonBillableCharge_TransCur, fc.NonBillableCharge_TransCur) AS NonBillableCharge_TransCur
         , ISNULL(tt.NonBillableCharge, fc.NonBillableCharge)                   AS NonBillableCharge
         , ISNULL(tt.TotalCharges_TransCur, fc.TotalCharges_TransCur)           AS TotalCharges_TransCur
         , ISNULL(tt.TotalCharges, fc.TotalCharges)                             AS TotalCharges
         , ISNULL(tt.TaxAmount_TransCur, fc.TaxAmount_TransCur)                 AS TaxAmount_TransCur
         , ISNULL(tt.TaxAmount, fc.TaxAmount)                                   AS TaxAmount
         , fc.IncludeInTotalPrice                                               AS IncludeInTotalPrice
         , fc.PrintCharges                                                      AS PrintCharges
         , ISNULL(fc._RecID1, 0)                                                AS _RecID1
         , ISNULL(fc._RecID2, 0)                                                AS _RecID2
         , ISNULL(tt._RecID3, 0)                                                AS _RecID3
         , 1                                                                    AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                    AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                    AS _ModifiedDate 
      FROM {{ ref('purchaseinvoicelinecharge_f') }}     fc
      LEFT JOIN purchaseinvoicelinechargetrans_facttrans4                            tt
        ON tt.PurchaseInvoiceLineChargeKey = fc.PurchaseInvoiceLineChargeKey
       AND tt.RecID_VIT                    = fc._RecID2
      LEFT JOIN {{ ref('purchaseinvoicelinetrans_f') }} pilt
        ON pilt._RecID3                    = ISNULL(tt._RecID3, 0)
       AND pilt._RecID2                    = fc._RecID2
       AND pilt._SourceID                  = 1;
