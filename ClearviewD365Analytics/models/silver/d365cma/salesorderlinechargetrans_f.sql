{{ config(materialized='table', tags=['silver'], alias='salesorderlinechargetrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/salesorderlinechargetrans_f/salesorderlinechargetrans_f.py
-- Root method: SalesorderlinechargetransFact.salesorderlinechargetrans_factdetail [SalesOrderLineChargeTrans_FactDetail]
-- Inlined methods: SalesorderlinechargetransFact.salesorderlinechargetrans_factsolkeys [SalesOrderLineChargeTrans_FactSOLKeys], SalesorderlinechargetransFact.salesorderlinechargetrans_factstage [SalesOrderLineChargeTrans_FactStage], SalesorderlinechargetransFact.salesorderlinechargetrans_factsolratio [SalesOrderLineChargeTrans_FactSOLRatio], SalesorderlinechargetransFact.salesorderlinechargetrans_facttrans [SalesOrderLineChargeTrans_FactTrans], SalesorderlinechargetransFact.salesorderlinechargetrans_factadj [SalesOrderLineChargeTrans_FactAdj], SalesorderlinechargetransFact.salesorderlinechargetrans_facttransadj [SalesOrderLineChargeTrans_FactTransAdj], SalesorderlinechargetransFact.salesorderlinechargetrans_facttrans2 [SalesOrderLineChargeTrans_FactTrans2], SalesorderlinechargetransFact.salesorderlinechargetrans_facttrans3 [SalesOrderLineChargeTrans_FactTrans3], SalesorderlinechargetransFact.salesorderlinechargetrans_facttrans4 [SalesOrderLineChargeTrans_FactTrans4]
-- external_table_name: SalesOrderLineChargeTrans_FactDetail
-- schema_name: temp

WITH
salesorderlinechargetrans_factsolkeys AS (
    SELECT t.RecID_IT
             , t.RecID_SL
             , t.ITEMID
             , t.OrderQty_SL
             , t.Qty_IT
             , t.CURRENCYCODE
             , t.RecID_MT
             , t.TaxCurrencyID
             , t.LegalEntityID

          FROM (   SELECT DISTINCT
                          it.recid             AS RecID_IT
                        , MAX (sl.recid)       AS RecID_SL
                        , MAX (sl.itemid)       AS ITEMID
                        , MAX (sl.qtyordered)   AS OrderQty_SL
                        , MAX (it.qty)          AS Qty_IT
                        , MAX (sl.currencycode) AS CURRENCYCODE
                        , mt.recid             AS RecID_MT
                        , MAX (mt.currencycode) AS TaxCurrencyID
                        , MAX (mt.dataareaid)  AS LegalEntityID
                     FROM {{ ref('markuptrans') }}            mt
                    INNER JOIN {{ ref('markuptable') }}       mu
                       ON mu.dataareaid       = mt.dataareaid
                      AND mu.markupcode        = mt.markupcode
                      AND mu.moduletype        = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }}     sd
                       ON sd.fieldid           = 0
                      AND sd.tabid             = mt.transtableid
                      AND sd.name              = 'SALESTABLE'
                    INNER JOIN {{ ref('salestable') }}        st
                       ON st.recid            = mt.transrecid
                    INNER JOIN {{ ref('salesline') }}         sl
                       ON sl.dataareaid       = st.dataareaid
                      AND sl.salesid           = st.salesid
                    INNER JOIN {{ ref('inventtransorigin') }} ito
                       ON ito.dataareaid      = sl.dataareaid
                      AND ito.inventtransid    = sl.inventtransid
                      AND ito.itemid           = sl.itemid
                    INNER JOIN {{ ref('inventtrans') }}       it
                       ON it.dataareaid       = ito.dataareaid
                      AND it.inventtransorigin = ito.recid
                      AND (it.statusissue IN ( 1, 2, 3, 4, 5, 6 ) OR it.statusreceipt IN ( 1, 2, 3, 4, 5 ))
                    GROUP BY it.recid
                           , mt.recid
                   UNION
                   SELECT DISTINCT
                          it.recid             AS RecID_IT
                        , MAX (sl.recid)       AS RecID_SL
                        , MAX (sl.itemid)       AS ITEMID
                        , MAX (sl.qtyordered)   AS OrderQty_SL
                        , MAX (it.qty)          AS Qty_IT
                        , MAX (sl.currencycode) AS CURRENCYCODE
                        , mt.recid             AS RecID_MT
                        , MAX (mt.currencycode) AS TaxCurrencyID
                        , MAX (mt.dataareaid)  AS LegalEntityID
                     FROM {{ ref('markuptrans') }}            mt
                    INNER JOIN {{ ref('markuptable') }}       mu
                       ON mu.dataareaid       = mt.dataareaid
                      AND mu.markupcode        = mt.markupcode
                      AND mu.moduletype        = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }}     sd
                       ON sd.fieldid           = 0
                      AND sd.tabid             = mt.transtableid
                      AND sd.name              = 'SalesLine'
                    INNER JOIN {{ ref('salesline') }}         sl
                       ON sl.recid            = mt.transrecid
                    INNER JOIN {{ ref('inventtransorigin') }} ito
                       ON ito.dataareaid     = sl.dataareaid
                      AND ito.inventtransid    = sl.inventtransid
                      AND ito.itemid           = sl.itemid
                    INNER JOIN {{ ref('inventtrans') }}       it
                       ON it.dataareaid       = ito.dataareaid
                      AND it.inventtransorigin = ito.recid
                      AND (it.statusissue IN ( 1, 2, 3, 4, 5, 6 ) OR it.statusreceipt IN ( 1, 2, 3, 4, 5 ))
                    GROUP BY it.recid
                           , mt.recid
                           ) AS t;
),
salesorderlinechargetrans_factstage AS (
    SELECT it.recid       AS RECID_IT
             , MAX (sl.recid) AS RECID_SL
             , MAX (it.qty)    AS QTY_IT

          FROM {{ ref('salesline') }}              sl
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid      = sl.dataareaid
           AND ito.inventtransid    = sl.inventtransid
           AND ito.itemid           = sl.itemid
         INNER JOIN {{ ref('inventtrans') }}       it
            ON it.inventtransorigin = ito.recid
         WHERE it.statusreceipt IN ( 1, 2, 3, 4, 5 )
            OR it.statusissue IN ( 1, 2, 3, 4, 5, 6 )
         GROUP BY it.recid;
),
salesorderlinechargetrans_factsolratio AS (
    SELECT tk.RECID_IT
             , CASE WHEN SUM (-1 * tk.QTY_IT) OVER (PARTITION BY tk.RECID_SL) = 0
                    THEN 1 / CAST(ISNULL (NULLIF(COUNT (1) OVER (PARTITION BY tk.RECID_SL), 0), 1) AS FLOAT)
                    ELSE
                    CAST(tk.QTY_IT AS FLOAT) * -1
                    / CAST(ISNULL (NULLIF(SUM (-1 * tk.QTY_IT) OVER (PARTITION BY tk.RECID_SL), 0), 1) AS FLOAT)END AS PercentOfTotal

          FROM salesorderlinechargetrans_factstage tk;
),
salesorderlinechargetrans_facttrans AS (
    SELECT fc.SalesOrderLineChargeKey
             , CAST(fc.IncludedCharge_TransCur * ISNULL (tr.PercentOfTotal, 1) AS MONEY)    AS IncludedCharge_TransCur
             , CAST(fc.IncludedCharge * ISNULL (tr.PercentOfTotal, 1) AS MONEY)             AS IncludedCharge
             , CAST(fc.AdditionalCharge_TransCur * ISNULL (tr.PercentOfTotal, 1) AS MONEY)  AS AdditionalCharge_TransCur
             , CAST(fc.AdditionalCharge * ISNULL (tr.PercentOfTotal, 1) AS MONEY)           AS AdditionalCharge
             , CAST(fc.NonBillableCharge_TransCur * ISNULL (tr.PercentOfTotal, 1) AS MONEY) AS NonBillableCharge_TransCur
             , CAST(fc.NonBillableCharge * ISNULL (tr.PercentOfTotal, 1) AS MONEY)          AS NonBillableCharge
             , CAST(fc.TotalCharges_TransCur * ISNULL (tr.PercentOfTotal, 1) AS MONEY)      AS TotalCharges_TransCur
             , CAST(fc.TotalCharges * ISNULL (tr.PercentOfTotal, 1) AS MONEY)               AS TotalCharges
             , CAST(fc.TaxAmount_TransCur * ISNULL (tr.PercentOfTotal, 1) AS MONEY)         AS TaxAmount_TransCur
             , CAST(fc.TaxAmount * ISNULL (tr.PercentOfTotal, 1) AS MONEY)                  AS TaxAmount
             , ISNULL (tk.RecID_IT, 0)                                                      AS _RecID3
             , CASE WHEN ROW_NUMBER () OVER (PARTITION BY fc.SalesOrderLineChargeKey
    ORDER BY ISNULL (tk.RecID_IT, 0), ISNULL (tk.RecID_MT, 0)) = 1
                    THEN 1
                    ELSE 0 END                                                              AS IsProrateAdj
             , tk.RecID_MT

          FROM {{ ref('salesorderlinecharge_f') }} fc
         INNER JOIN salesorderlinechargetrans_factsolkeys                 tk
            ON tk.RecID_MT = fc._RecID1
           AND tk.RecID_SL = fc._RecID2
         INNER JOIN salesorderlinechargetrans_factsolratio                tr
            ON tr.RECID_IT = tk.RecID_IT;
),
salesorderlinechargetrans_factadj AS (
    SELECT t.SalesOrderLineChargeKey
             , t.RecID_MT
             , t._RecID3
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.IncludedCharge_TransCur
                              - SUM (t.IncludedCharge_TransCur) OVER (PARTITION BY t.SalesOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS IncludedCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.IncludedCharge - SUM (t.IncludedCharge) OVER (PARTITION BY t.SalesOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS IncludedChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.AdditionalCharge_TransCur
                              - SUM (t.AdditionalCharge_TransCur) OVER (PARTITION BY t.SalesOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS AdditionalCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.AdditionalCharge - SUM (t.AdditionalCharge) OVER (PARTITION BY t.SalesOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS AdditionalChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NonBillableCharge
                              - SUM (t.NonBillableCharge) OVER (PARTITION BY t.SalesOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS NonBillableChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NonBillableCharge_TransCur
                              - SUM (t.NonBillableCharge_TransCur) OVER (PARTITION BY t.SalesOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS NonBillableCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TotalCharges_TransCur
                              - SUM (t.TotalCharges_TransCur) OVER (PARTITION BY t.SalesOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS TotalCharges_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TotalCharges - SUM (t.TotalCharges) OVER (PARTITION BY t.SalesOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS TotalChargesAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TaxAmount_TransCur
                              - SUM (t.TaxAmount_TransCur) OVER (PARTITION BY t.SalesOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS TaxAmount_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TaxAmount - SUM (t.TaxAmount) OVER (PARTITION BY t.SalesOrderLineChargeKey)
                         ELSE 0 END AS MONEY) AS TaxAmountAdj
          FROM salesorderlinechargetrans_facttrans                             t
         INNER JOIN {{ ref('salesorderlinecharge_f') }} fcl
            ON fcl.SalesOrderLineChargeKey = t.SalesOrderLineChargeKey
),
salesorderlinechargetrans_facttransadj AS (
    SELECT SalesOrderLineChargeKey
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
          FROM salesorderlinechargetrans_factadj
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
salesorderlinechargetrans_facttrans2 AS (
    SELECT 
          t.SalesOrderLineChargeKey
          ,t.IncludedCharge_TransCur + ta.IncludedCharge_TransCurAdj AS IncludedCharge_TransCur
             , t.IncludedCharge + ta.IncludedChargeAdj AS IncludedCharge
             , t.AdditionalCharge_TransCur + ta.AdditionalCharge_TransCurAdj AS AdditionalCharge_TransCur
             , t.AdditionalCharge + ta.AdditionalChargeAdj AS AdditionalCharge
             , t.NonBillableCharge_TransCur + ta.NonBillableCharge_TransCurAdj AS NonBillableCharge_TransCur
             , t.NonBillableCharge + ta.NonBillableChargeAdj AS NonBillableCharge
             , t.TotalCharges_TransCur + ta.TotalCharges_TransCurAdj AS TotalCharges_TransCur
             , t.TotalCharges + ta.TotalChargesAdj As TotalCharges
             , t.TaxAmount_TransCur + ta.TaxAmount_TransCurAdj AS TaxAmount_TransCur
             , t.TaxAmount + ta.TaxAmountAdj AS TaxAmount
             , t._RecID3
             , t.IsProrateAdj
             , t.RecID_MT
          FROM salesorderlinechargetrans_facttrans        t
         INNER JOIN salesorderlinechargetrans_facttransadj ta
            ON ta.SalesOrderLineChargeKey = t.SalesOrderLineChargeKey
           AND ta._RecID3                 = t._RecID3;
),
salesorderlinechargetrans_facttrans3 AS (
    SELECT 
           t.SalesOrderLineChargeKey
          ,t.IncludedCharge_TransCur 
             , t.IncludedCharge 
             , t.AdditionalCharge_TransCur 
             , t.AdditionalCharge 
             , t.NonBillableCharge_TransCur 
             , t.NonBillableCharge 
             , t.TotalCharges_TransCur 
             , t.TotalCharges 
             , t.TaxAmount_TransCur
             , t.TaxAmount 
             , t._RecID3
             , t.IsProrateAdj
             , t.RecID_MT
          FROM salesorderlinechargetrans_facttrans        t
         LEFT JOIN salesorderlinechargetrans_facttransadj ta
            ON ta.SalesOrderLineChargeKey = t.SalesOrderLineChargeKey
           AND ta._RecID3                 = t._RecID3
           WHERE ta._RecID3 IS NULL
),
salesorderlinechargetrans_facttrans4 AS (
    SELECT * FROM salesorderlinechargetrans_facttrans2
        UNION ALL
            SELECT * FROM salesorderlinechargetrans_facttrans3
)
SELECT 
           CURRENT_TIMESTAMP                                                                      AS _CreatedDate
         , CURRENT_TIMESTAMP                                                                      AS _ModifiedDate 
         , ROW_NUMBER() OVER (ORDER BY t._RecID1) AS SalesOrderLineChargeTransKey
         , * FROM ( SELECT DISTINCT
          fc.SalesOrderLineChargeKey
         , ISNULL (solt.SalesOrderLineTransKey, -1)                              AS SalesOrderLineTransKey
         , fc.LegalEntityKey                                                     AS LegalEntityKey
         , ISNULL (tt.IncludedCharge_TransCur, fc.IncludedCharge_TransCur)       AS IncludedCharge_TransCur
         , ISNULL (tt.IncludedCharge, fc.IncludedCharge)                         AS IncludedCharge
         , ISNULL (tt.AdditionalCharge_TransCur, fc.AdditionalCharge_TransCur)   AS AdditionalCharge_TransCur
         , ISNULL (tt.AdditionalCharge, fc.AdditionalCharge)                     AS AdditionalCharge
         , ISNULL (tt.NonBillableCharge_TransCur, fc.NonBillableCharge_TransCur) AS NonBillableCharge_TransCur
         , ISNULL (tt.NonBillableCharge, fc.NonBillableCharge)                   AS NonBillableCharge
         , ISNULL (tt.TotalCharges_TransCur, fc.TotalCharges_TransCur)           AS TotalCharges_TransCur
         , ISNULL (tt.TotalCharges, fc.TotalCharges)                             AS TotalCharges
         , ISNULL (tt.TaxAmount_TransCur, fc.TaxAmount_TransCur)                 AS TaxAmount_TransCur
         , ISNULL (tt.TaxAmount, fc.TaxAmount)                                   AS TaxAmount
         , fc.IncludeInTotalPrice                                                AS IncludeInTotalPrice
         , fc.PrintCharges                                                       AS PrintCharges
         , ISNULL (tt._RecID3, 0)                                                AS _RecID3
         , ISNULL (fc._RecID1, 0)                                                AS _RECID1
         , ISNULL (fc._RecID2, 0)                                                AS _RecID2
         , 1                                                                     AS _SourceID
      FROM {{ ref('salesorderlinecharge_f') }}     fc
           LEFT JOIN salesorderlinechargetrans_facttrans4                       tt
        ON tt.SalesOrderLineChargeKey = fc.SalesOrderLineChargeKey
      LEFT JOIN {{ ref('salesorderlinetrans_f') }} solt
        ON solt._RecID2               = ISNULL (tt._RecID3, 0)
       AND solt._RecID1               = fc._RecID2
       AND solt._SourceID             = 1 ) t;
