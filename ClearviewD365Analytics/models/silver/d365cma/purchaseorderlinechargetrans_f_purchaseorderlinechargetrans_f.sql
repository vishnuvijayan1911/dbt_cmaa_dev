{{ config(materialized='table', tags=['silver'], alias='purchaseorderlinechargetrans_fact_purchaseorderlinechargetrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseorderlinechargetrans_f/purchaseorderlinechargetrans_f.py
-- Root method: PurchaseorderlinechargetransFact.purchaseorderlinechargetrans_facttransadj [PurchaseOrderLineChargeTrans_FactTransAdj]
-- Inlined methods: PurchaseorderlinechargetransFact.purchaseorderlinechargetrans_factpolkeys [PurchaseOrderLineChargeTrans_FactPOLKeys], PurchaseorderlinechargetransFact.purchaseorderlinechargetrans_factstage [PurchaseOrderLineChargeTrans_FactStage], PurchaseorderlinechargetransFact.purchaseorderlinechargetrans_factpolratio [PurchaseOrderLineChargeTrans_FactPOLRatio], PurchaseorderlinechargetransFact.purchaseorderlinechargetrans_facttrans [PurchaseOrderLineChargeTrans_FactTrans], PurchaseorderlinechargetransFact.purchaseorderlinechargetrans_factadj [PurchaseOrderLineChargeTrans_FactAdj]
-- external_table_name: PurchaseOrderLineChargeTrans_FactTransAdj
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

          FROM {{ ref('purchaseorderlinecharge_f') }} fc
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
         INNER JOIN {{ ref('purchaseorderlinecharge_f') }} fcl
            ON fcl.PurchaseOrderLineChargeKey = t.PurchaseOrderLineChargeKey
)
SELECT PurchaseOrderLineChargeKey
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

      FROM purchaseorderlinechargetrans_factadj
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
