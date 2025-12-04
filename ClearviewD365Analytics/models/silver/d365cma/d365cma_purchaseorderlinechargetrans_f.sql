{{ config(materialized='table', tags=['silver'], alias='purchaseorderlinechargetrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseorderlinechargetrans_f/purchaseorderlinechargetrans_f.py
-- Root method: PurchaseorderlinechargetransFact.purchaseorderlinechargetrans_factdetail [PurchaseOrderLineChargeTrans_FactDetail]
-- Inlined methods: PurchaseorderlinechargetransFact.purchaseorderlinechargetrans_factpolkeys [PurchaseOrderLineChargeTrans_FactPOLKeys], PurchaseorderlinechargetransFact.purchaseorderlinechargetrans_factstage [PurchaseOrderLineChargeTrans_FactStage], PurchaseorderlinechargetransFact.purchaseorderlinechargetrans_factpolratio [PurchaseOrderLineChargeTrans_FactPOLRatio], PurchaseorderlinechargetransFact.purchaseorderlinechargetrans_facttrans [PurchaseOrderLineChargeTrans_FactTrans], PurchaseorderlinechargetransFact.purchaseorderlinechargetrans_factadj [PurchaseOrderLineChargeTrans_FactAdj], PurchaseorderlinechargetransFact.purchaseorderlinechargetrans_facttrans2 [PurchaseOrderLineChargeTrans_FactTrans2], PurchaseorderlinechargetransFact.purchaseorderlinechargetrans_facttrans3 [PurchaseOrderLineChargeTrans_FactTrans3], PurchaseorderlinechargetransFact.purchaseorderlinechargetrans_facttrans4 [PurchaseOrderLineChargeTrans_FactTrans4]
-- external_table_name: PurchaseOrderLineChargeTrans_FactDetail
-- schema_name: temp

WITH
purchaseorderlinechargetrans_factpolkeys AS (
    SELECT t.RecID_IT
             , t.RecID_PL
             , t.OrderQty_PL
             , t.Qty_IT
             , t.ItemID
             , t.RecID_MT
             , t.TaxCurrencyID
             , t.LegalEntityID

          FROM (   SELECT DISTINCT
                          it.recid             AS RecID_IT
                        , MAX(pl.recid)        AS RecID_PL
                        , MAX(pl.qtyordered)   AS OrderQty_PL
                        , MAX(it.qty)          AS Qty_IT
                        , MAX(pl.itemid)       AS ItemID
                        , mt.recid             AS RecID_MT
                        , MAX(mt.currencycode) AS TaxCurrencyID
                        , MAX(mt.dataareaid)   AS LegalEntityID
                     FROM {{ ref('markuptrans') }}            mt

                    INNER JOIN {{ ref('markuptable') }}       mu
                       ON mu.dataareaid        = mt.dataareaid
                      AND mu.markupcode        = mt.markupcode
                      AND mu.moduletype        = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }}     sd
                       ON sd.fieldid           = 0
                      AND sd.tabid           = mt.transtableid
                      AND sd.name              = 'PurchTable'
                    INNER JOIN {{ ref('purchtable') }}        ph
                       ON ph.recid             = mt.transrecid
                    INNER JOIN {{ ref('purchline') }}         pl
                       ON pl.dataareaid        = ph.dataareaid
                      AND pl.purchid           = ph.purchid
                    INNER JOIN {{ ref('inventtransorigin') }} ito
                       ON ito.dataareaid       = pl.dataareaid
                      AND ito.inventtransid    = pl.inventtransid
                      AND ito.itemid           = pl.itemid
                    INNER JOIN {{ ref('inventtrans') }}       it
                       ON it.dataareaid        = ito.dataareaid
                      AND it.inventtransorigin = ito.recid
                      AND (it.statusreceipt IN ( 1, 2, 3, 4, 5 ) OR it.statusissue IN ( 1, 2, 3, 4, 5, 6 ))
                    GROUP BY it.recid
                           , mt.recid
                   UNION
                   SELECT DISTINCT
                          it.recid             AS RecID_IT
                        , MAX(pl.recid)        AS RecID_PL
                        , MAX(pl.qtyordered)   AS OrderQty_PL
                        , MAX(it.qty)          AS Qty_IT
                        , MAX(pl.itemid)       AS ItemID
                        , mt.recid             AS RecID_MT
                        , MAX(mt.currencycode) AS TaxCurrencyID
                        , MAX(mt.dataareaid)   AS LegalEntityID
                     FROM {{ ref('markuptrans') }}            mt
                    INNER JOIN {{ ref('markuptable') }}       mu
                       ON mu.dataareaid        = mt.dataareaid
                      AND mu.markupcode        = mt.markupcode
                      AND mu.moduletype        = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }}     sd
                       ON sd.fieldid           = 0
                      AND sd.tabid          = mt.transtableid
                      AND sd.name              = 'PURCHLINE'
                    INNER JOIN {{ ref('purchline') }}         pl
                       ON pl.recid             = mt.transrecid
                    INNER JOIN {{ ref('inventtransorigin') }} ito
                       ON ito.dataareaid       = pl.dataareaid
                      AND ito.inventtransid    = pl.inventtransid
                      AND ito.itemid           = pl.itemid
                    INNER JOIN {{ ref('inventtrans') }}       it
                       ON it.dataareaid       = ito.dataareaid
                      AND it.inventtransorigin = ito.recid
                      AND (it.statusreceipt IN ( 1, 2, 3, 4, 5 ) OR it.statusissue IN ( 1, 2, 3, 4, 5, 6 ))
                    GROUP BY it.recid
                           , mt.recid) AS t;
),
purchaseorderlinechargetrans_factstage AS (
    SELECT it.recid      AS RECID_IT
             , MAX(pl.recid) AS RECID_PL
             , MAX(it.qty)   AS QTY_IT

          FROM {{ ref('purchline') }}              pl
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid       = pl.dataareaid
           AND ito.inventtransid    = pl.inventtransid
           AND ito.itemid           = pl.itemid
         INNER JOIN {{ ref('inventtrans') }}       it
            ON it.inventtransorigin = ito.recid
         WHERE it.statusreceipt IN ( 1, 2, 3, 4, 5 )
            OR it.statusissue IN ( 1, 2, 3, 4, 5, 6 )
         GROUP BY it.recid;
),
purchaseorderlinechargetrans_factpolratio AS (
    SELECT DISTINCT
               tk.RECID_IT
             , CASE WHEN SUM(tk.QTY_IT) OVER (PARTITION BY tk.RECID_PL) = 0
                    THEN 1 / CAST(ISNULL(NULLIF(COUNT(1) OVER (PARTITION BY tk.RECID_PL), 0), 1) AS FLOAT)
                    ELSE
                    CAST(tk.QTY_IT AS FLOAT)
                    / CAST(ISNULL(NULLIF(SUM(tk.QTY_IT) OVER (PARTITION BY tk.RECID_PL), 0), 1) AS FLOAT)END AS PercentOfTotal

          FROM purchaseorderlinechargetrans_factstage tk;
),
purchaseorderlinechargetrans_facttrans AS (
    SELECT fc.PurchaseOrderLineChargeKey
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
             , CASE WHEN ROW_NUMBER() OVER (PARTITION BY fc.PurchaseOrderLineChargeKey
                                                ORDER BY ISNULL(tk.RecID_IT, 0)
                                                       , ISNULL(tk.RecID_MT, 0)) = 1
                    THEN 1
                    ELSE 0 END                                                             AS IsProrateAdj
             , tk.RecID_MT

          FROM {{ ref('d365cma_purchaseorderlinecharge_f') }} fc
         INNER JOIN purchaseorderlinechargetrans_factpolkeys                    tk
            ON tk.RecID_MT = fc._RecID1
           AND tk.RecID_PL = fc._RECID2
         INNER JOIN purchaseorderlinechargetrans_factpolratio                   tr
            ON tr.RecID_IT = tk.RecID_IT;
),
purchaseorderlinechargetrans_factadj AS (
    SELECT t.PurchaseOrderLineChargeKey
             , t.RecID_MT
             , t._RecID3
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.IncludedCharge_TransCur
                              - SUM(t.IncludedCharge_TransCur) OVER (PARTITION BY t.PurchaseOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS IncludedCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.IncludedCharge - SUM(t.IncludedCharge) OVER (PARTITION BY t.PurchaseOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS IncludedChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.AdditionalCharge_TransCur
                              - SUM(t.AdditionalCharge_TransCur) OVER (PARTITION BY t.PurchaseOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS AdditionalCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.AdditionalCharge
                              - SUM(t.AdditionalCharge) OVER (PARTITION BY t.PurchaseOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS AdditionalChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NonBillableCharge
                              - SUM(t.NonBillableCharge) OVER (PARTITION BY t.PurchaseOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS NonBillableChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NonBillableCharge_TransCur
                              - SUM(t.NonBillableCharge_TransCur) OVER (PARTITION BY t.PurchaseOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS NonBillableCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TotalCharges_TransCur
                              - SUM(t.TotalCharges_TransCur) OVER (PARTITION BY t.PurchaseOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS TotalCharges_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TotalCharges - SUM(t.TotalCharges) OVER (PARTITION BY t.PurchaseOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS TotalChargesAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TaxAmount_TransCur
                              - SUM(t.TaxAmount_TransCur) OVER (PARTITION BY t.PurchaseOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS TaxAmount_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TaxAmount - SUM(t.TaxAmount) OVER (PARTITION BY t.PurchaseOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS TaxAmountAdj

          FROM purchaseorderlinechargetrans_facttrans                                t
         INNER JOIN {{ ref('d365cma_purchaseorderlinecharge_f') }} fcl
            ON fcl.PurchaseOrderLineChargeKey = t.PurchaseOrderLineChargeKey
),
purchaseorderlinechargetrans_facttrans2 AS (
    SELECT t.PurchaseOrderLineChargeKey
             , t.IncludedCharge_TransCur + ta.IncludedCharge_TransCurAdj       AS IncludedCharge_TransCur
             , t.IncludedCharge + ta.IncludedChargeAdj                         AS IncludedCharge
             , t.AdditionalCharge_TransCur + ta.AdditionalCharge_TransCurAdj   AS AdditionalCharge_TransCur
             , t.AdditionalCharge + ta.AdditionalChargeAdj                     AS AdditionalCharge
             , t.NonBillableCharge + ta.NonBillableChargeAdj                   AS NonBillableCharge
             , t.NonBillableCharge_TransCur + ta.NonBillableCharge_TransCurAdj         AS NonBillableCharge_TransCur
             , t.TotalCharges_TransCur + ta.TotalCharges_TransCurAdj           AS TotalCharges_TransCur
             , t.TotalCharges + ta.TotalChargesAdj                             AS TotalCharges
             ,  t.TaxAmount_TransCur + ta.TaxAmount_TransCurAdj                AS TaxAmount_TransCur
             ,  t.TaxAmount + ta.TaxAmountAdj                                  AS TaxAmount
             ,  t._RecID3
             , t.IsProrateAdj
             , t.RecID_MT

           FROM purchaseorderlinechargetrans_facttrans         t
         INNER JOIN purchaseorderlinechargetrans_factadj ta
            ON ta.PurchaseOrderLineChargeKey = t.PurchaseOrderLineChargeKey
           AND ta._RecID3                    = t._RecID3;
),
purchaseorderlinechargetrans_facttrans3 AS (
    SELECT t.PurchaseOrderLineChargeKey
             , t.IncludedCharge_TransCur
             , t.IncludedCharge
             , t.AdditionalCharge_TransCur
             , t.AdditionalCharge
             , t.NonBillableCharge
             , t.NonBillableCharge_TransCur
             , t.TotalCharges_TransCur
             , t.TotalCharges
             , t.TaxAmount_TransCur
             , t.TaxAmount
             , t._RecID3
             , t.IsProrateAdj
             , t.RecID_MT

           FROM purchaseorderlinechargetrans_facttrans         t
         LEFT JOIN purchaseorderlinechargetrans_factadj ta
            ON ta.PurchaseOrderLineChargeKey = t.PurchaseOrderLineChargeKey
           AND ta._RecID3                    = t._RecID3
           WHERE ta.PurchaseOrderLineChargeKey IS NULL
),
purchaseorderlinechargetrans_facttrans4 AS (
    SELECT * FROM purchaseorderlinechargetrans_facttrans2
       UNION ALL
      SELECT * FROM purchaseorderlinechargetrans_facttrans3
)
SELECT 
           {{ dbt_utils.generate_surrogate_key(['t.IncludeInTotalPrice']) }} AS PurchaseOrderLineChargeTransKey
         , * FROM ( SELECT DISTINCT
           fc.PurchaseOrderLineChargeKey
         , ISNULL(polt.PurchaseOrderLineTransKey, -1)                           AS PurchaseOrderLineTransKey
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

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                      AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                      AS _ModifiedDate 
      FROM {{ ref('d365cma_purchaseorderlinecharge_f') }}     fc
      LEFT JOIN purchaseorderlinechargetrans_facttrans4                          tt
        ON tt.PurchaseOrderLineChargeKey = fc.PurchaseOrderLineChargeKey
      LEFT JOIN {{ ref('d365cma_purchaseorderlinetrans_f') }} polt
        ON polt._RecID2                  = ISNULL(tt._RecID3, 0)
       AND polt._RecID1                  = fc._RecID2
       AND polt._SourceID                = 1) t;
