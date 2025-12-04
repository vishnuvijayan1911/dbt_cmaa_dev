{{ config(materialized='table', tags=['silver'], alias='salesinvoicelinechargetrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/salesinvoicelinechargetrans_f/salesinvoicelinechargetrans_f.py
-- Root method: SalesinvoicelinechargetransFact.salesinvoicelinechargetrans_factdetail [SalesInvoiceLineChargeTrans_FactDetail]
-- Inlined methods: SalesinvoicelinechargetransFact.salesinvoicelinechargetrans_factsilkeys [SalesInvoiceLineChargeTrans_FactSilKeys], SalesinvoicelinechargetransFact.salesinvoicelinechargetrans_factstage [SalesInvoiceLineChargeTrans_FactStage], SalesinvoicelinechargetransFact.salesinvoicelinechargetrans_factsilratio [SalesInvoiceLineChargeTrans_FactSILRatio], SalesinvoicelinechargetransFact.salesinvoicelinechargetrans_facttrans [SalesInvoiceLineChargeTrans_FactTrans], SalesinvoicelinechargetransFact.salesinvoicelinechargetrans_factadj [SalesInvoiceLineChargeTrans_FactAdj], SalesinvoicelinechargetransFact.salesinvoicelinechargetrans_facttransadj [SalesInvoiceLineChargeTrans_FactTransAdj], SalesinvoicelinechargetransFact.salesinvoicelinechargetrans_facttrans2 [SalesInvoiceLineChargeTrans_FactTrans2], SalesinvoicelinechargetransFact.salesinvoicelinechargetrans_facttrans3 [SalesInvoiceLineChargeTrans_FactTrans3], SalesinvoicelinechargetransFact.salesinvoicelinechargetrans_facttrans4 [SalesInvoiceLineChargeTrans_FactTrans4]
-- external_table_name: SalesInvoiceLineChargeTrans_FactDetail
-- schema_name: temp

WITH
salesinvoicelinechargetrans_factsilkeys AS (
    SELECT t.RECID_IT
             , t.RECID_CIT
             , t.InventQty_CIT
             , t.ItemID
             , t.Qty_IT
             , t.RECID_MT
             , t.ChargeCurrencyID
             , t.LegalEntityID

          FROM (   SELECT it.recid             AS RECID_IT
                        , MAX(cit.recid)       AS RECID_CIT
                        , MAX(cit.inventqty)   AS InventQty_CIT
                        , MAX(cit.itemid)      AS ItemID
                        , MAX(it.qty)          AS Qty_IT
                        , mt.recid             AS RECID_MT
                        , MAX(mt.currencycode) AS ChargeCurrencyID
                        , MAX(mt.dataareaid)   AS LegalEntityID
                     FROM {{ ref('markuptrans') }}            mt
                    INNER JOIN {{ ref('markuptable') }}       mu
                       ON mu.dataareaid           = mt.dataareaid
                      AND mu.markupcode           = mt.markupcode
                      AND mu.moduletype           = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }}     sd
                       ON sd.fieldid              = 0
                      AND sd.tabid                = mt.transtableid
                      AND sd.name                 = 'CUSTINVOICETRANS'
                    INNER JOIN {{ ref('custinvoicetrans') }}  cit
                       ON cit.recid               = mt.transrecid
                    INNER JOIN {{ ref('custinvoicejour') }}   cij
                       ON cij.dataareaid          = cit.dataareaid
                      AND cij.salesid             = cit.salesid
                      AND cij.invoiceid           = cit.invoiceid
                      AND cij.invoicedate         = cit.invoicedate
                      AND cij.numbersequencegroup = cit.numbersequencegroup
                      AND (cij.recid              = cit.parentrecid OR cij.salestype <> 0)
                    INNER JOIN {{ ref('inventtransorigin') }} ito
                       ON ito.dataareaid          = cit.dataareaid
                      AND ito.inventtransid       = cit.inventtransid
                      AND ito.itemid              = cit.itemid
                      AND ito.referencecategory   = 0
                    INNER JOIN {{ ref('inventtrans') }}       it
                       ON it.dataareaid           = ito.dataareaid
                      AND it.inventtransorigin    = ito.recid
                      AND it.itemid               = ito.itemid
                      AND it.invoiceid            = cit.invoiceid
                    GROUP BY it.recid
                           , mt.recid
                   UNION
                   SELECT it.recid             AS RECID_IT
                        , MAX(cit.recid)       AS RECID_CIT
                        , MAX(cit.inventqty)   AS InventQty_CIT
                        , MAX(cit.itemid)      AS ItemID
                        , MAX(it.qty)          AS Qty_IT
                        , mt.recid             AS RECID_MT
                        , MAX(mt.currencycode) AS ChargeCurrencyID
                        , MAX(mt.dataareaid)   AS LegalEntityID
                     FROM {{ ref('markuptrans') }}            mt
                    INNER JOIN {{ ref('markuptable') }}       mu
                       ON mu.dataareaid           = mt.dataareaid
                      AND mu.markupcode           = mt.markupcode
                      AND mu.moduletype           = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }}     sd
                       ON sd.fieldid              = 0
                      AND sd.tabid                = mt.transtableid
                      AND sd.name                 = 'CustInvoiceJour'
                    INNER JOIN {{ ref('custinvoicejour') }}   cij
                       ON cij.recid               = mt.transrecid
                    INNER JOIN {{ ref('custinvoicetrans') }}  cit
                       ON cit.dataareaid          = cij.dataareaid
                      AND cit.salesid             = cij.salesid
                      AND cit.invoiceid           = cij.invoiceid
                      AND cit.invoicedate         = cij.invoicedate
                      AND cit.numbersequencegroup = cij.numbersequencegroup
                    INNER JOIN {{ ref('inventtransorigin') }} ito
                       ON ito.dataareaid          = cit.dataareaid
                      AND ito.inventtransid       = cit.inventtransid
                      AND ito.itemid              = cit.itemid
                      AND ito.referencecategory   = 0
                    INNER JOIN {{ ref('inventtrans') }}       it
                       ON it.dataareaid           = ito.dataareaid
                      AND it.inventtransorigin    = ito.recid
                      AND it.itemid               = ito.itemid
                      AND it.invoiceid            = cit.invoiceid
                    GROUP BY it.recid
                           , mt.recid
                           ) AS t;
),
salesinvoicelinechargetrans_factstage AS (
    SELECT it.recid      AS RECID_IT
             , MAX(pl.recid) AS RECID_CIT
             , MAX(it.qty)   AS QTY_IT

          FROM {{ ref('custinvoicetrans') }}       pl
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid        = pl.dataareaid
           AND ito.inventtransid     = pl.inventtransid
           AND ito.itemid            = pl.itemid
           AND ito.referencecategory = 0
         INNER JOIN {{ ref('inventtrans') }}       it
            ON it.dataareaid         = ito.dataareaid
           AND it.inventtransorigin  = ito.recid
           AND it.itemid             = ito.itemid
           AND it.invoiceid          = pl.invoiceid
         GROUP BY it.recid;
),
salesinvoicelinechargetrans_factsilratio AS (
    SELECT tk.RECID_IT
             , CASE WHEN SUM(-1 * tk.QTY_IT) OVER (PARTITION BY tk.RECID_CIT) = 0
                    THEN 1 / CAST(ISNULL(NULLIF(COUNT(1) OVER (PARTITION BY tk.RECID_CIT), 0), 1) AS FLOAT)
                    ELSE
                    CAST(tk.QTY_IT AS FLOAT) * -1
                    / CAST(ISNULL(NULLIF(SUM(-1 * tk.QTY_IT) OVER (PARTITION BY tk.RECID_CIT), 0), 1) AS FLOAT)END AS PercentOfTotal

          FROM salesinvoicelinechargetrans_factstage tk;
),
salesinvoicelinechargetrans_facttrans AS (
    SELECT fc.SalesInvoiceLineChargeKey
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
             , ISNULL(tk.RECID_IT, 0)                                                      AS _RecID3
             , CASE WHEN ROW_NUMBER() OVER (PARTITION BY fc.SalesInvoiceLineChargeKey
                                                ORDER BY ISNULL(tk.RECID_IT, 0)
                                                       , ISNULL(tk.RECID_MT, 0)) = 1
                    THEN 1
                    ELSE 0 END                                                             AS IsProrateAdj
             , tk.RECID_MT
             , tk.RECID_CIT

          FROM {{ ref('d365cma_salesinvoicelinecharge_f') }} fc
         INNER JOIN salesinvoicelinechargetrans_factsilkeys                   tk
            ON tk.RECID_MT  = fc._RECID1
           AND tk.RECID_CIT = fc._RECID2
         INNER JOIN salesinvoicelinechargetrans_factsilratio                  tr
            ON tr.RECID_IT  = tk.RECID_IT;
),
salesinvoicelinechargetrans_factadj AS (
    SELECT t.SalesInvoiceLineChargeKey
             , t.RECID_MT
             , t._RecID3
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.IncludedCharge_TransCur
                              - SUM(t.IncludedCharge_TransCur) OVER (PARTITION BY t.SalesInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS IncludedCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.IncludedCharge - SUM(t.IncludedCharge) OVER (PARTITION BY t.SalesInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS IncludedChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.AdditionalCharge_TransCur
                              - SUM(t.AdditionalCharge_TransCur) OVER (PARTITION BY t.SalesInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS AdditionalCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.AdditionalCharge
                              - SUM(t.AdditionalCharge) OVER (PARTITION BY t.SalesInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS AdditionalChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NonBillableCharge
                              - SUM(t.NonBillableCharge) OVER (PARTITION BY t.SalesInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS NonBillableChargeAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.NonBillableCharge_TransCur
                              - SUM(t.NonBillableCharge_TransCur) OVER (PARTITION BY t.SalesInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS NonBillableCharge_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TotalCharges_TransCur
                              - SUM(t.TotalCharges_TransCur) OVER (PARTITION BY t.SalesInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS TotalCharges_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TotalCharges - SUM(t.TotalCharges) OVER (PARTITION BY t.SalesInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS TotalChargesAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TaxAmount_TransCur
                              - SUM(t.TaxAmount_TransCur) OVER (PARTITION BY t.SalesInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS TaxAmount_TransCurAdj
             , CAST(CASE WHEN t.IsProrateAdj = 1
                         THEN fcl.TaxAmount - SUM(t.TaxAmount) OVER (PARTITION BY t.SalesInvoiceLineChargeKey)
                         ELSE 0 END AS MONEY) AS TaxAmountAdj

          FROM salesinvoicelinechargetrans_facttrans                               t
         INNER JOIN {{ ref('d365cma_salesinvoicelinecharge_f') }} fcl
            ON fcl.SalesInvoiceLineChargeKey = t.SalesInvoiceLineChargeKey
),
salesinvoicelinechargetrans_facttransadj AS (
    SELECT SalesInvoiceLineChargeKey
             , _RecID3
             , RECID_MT
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

          FROM salesinvoicelinechargetrans_factadj
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
salesinvoicelinechargetrans_facttrans2 AS (
    SELECT t.SalesInvoiceLineChargeKey
             ,t.IncludedCharge_TransCur + ta.IncludedCharge_TransCurAdj              AS IncludedCharge_TransCur
             , t.IncludedCharge + ta.IncludedChargeAdj                               AS IncludedCharge
             , t.AdditionalCharge_TransCur + ta.AdditionalCharge_TransCurAdj         AS AdditionalCharge_TransCur
             ,t.AdditionalCharge + ta.AdditionalChargeAdj                            AS AdditionalCharge
             , t.NonBillableCharge_TransCur + ta.NonBillableCharge_TransCurAdj       AS NonBillableCharge_TransCur
             , t.NonBillableCharge + ta.NonBillableChargeAdj                         AS NonBillableCharge
             , t.TotalCharges_TransCur + ta.TotalCharges_TransCurAdj                 AS TotalCharges_TransCur
             , t.TotalCharges + ta.TotalChargesAdj                                   AS TotalCharges
             , t.TaxAmount_TransCur + ta.TaxAmount_TransCurAdj                       AS TaxAmount_TransCur
             , t.TaxAmount + ta.TaxAmountAdj                                         AS TaxAmount
             , t._RecID3
             , t.IsProrateAdj
             , t.RECID_MT
             , t.RECID_CIT


           FROM salesinvoicelinechargetrans_facttrans         t
         INNER JOIN salesinvoicelinechargetrans_facttransadj ta
            ON ta.SalesInvoiceLineChargeKey = t.SalesInvoiceLineChargeKey
           AND ta._RecID3                   = t._RecID3;
),
salesinvoicelinechargetrans_facttrans3 AS (
    SELECT  t.SalesInvoiceLineChargeKey
             , t.IncludedCharge_TransCur
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
             , t.RECID_MT
             , t.RECID_CIT

           FROM salesinvoicelinechargetrans_facttrans         t
          LEFT JOIN salesinvoicelinechargetrans_facttransadj ta
            ON ta.SalesInvoiceLineChargeKey = t.SalesInvoiceLineChargeKey
           AND ta._RecID3                   = t._RecID3
            WHERE ta.SalesInvoiceLineChargeKey IS NULL
),
salesinvoicelinechargetrans_facttrans4 AS (
    SELECT * FROM salesinvoicelinechargetrans_facttrans2
       UNION ALL
       SELECT * FROM salesinvoicelinechargetrans_facttrans3
)
SELECT DISTINCT
         , fc.SalesInvoiceLineChargeKey
         , ISNULL(fclt.SalesInvoiceLineTransKey, -1)                            AS SalesInvoiceLineTransKey
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
         , ISNULL(fc._RECID1, 0)                                                AS _RECID1
         , ISNULL(fc._RECID2, 0)                                                AS _RecID2
         , ISNULL(tt._RecID3, 0)                                                AS _RecID3
         , 1                                                                    AS _SourceID

           cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                      AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                      AS _ModifiedDate 
      FROM {{ ref('d365cma_salesinvoicelinecharge_f') }}     fc

      LEFT JOIN salesinvoicelinechargetrans_facttrans4                         tt
        ON tt.SalesInvoiceLineChargeKey = fc.SalesInvoiceLineChargeKey
       AND tt.RECID_CIT                 = fc._RecID2
      LEFT JOIN {{ ref('d365cma_salesinvoicelinetrans_f') }} fclt
        ON fclt._RecID3                 = ISNULL(tt._RecID3, 0)
       AND fclt._RECID2                 = fc._RecID2
       AND fclt._SourceID               = 1
     INNER JOIN {{ ref('d365cma_salesinvoiceline_d') }}           sil
        ON sil.SalesInvoiceLineKey      = fc.SalesInvoiceLineKey;
