{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/salesinvoicelinetrans_fact/salesinvoicelinetrans_fact.py
-- Root method: SalesinvoicelinetransFact.salesinvoicelinetrans_factdetail [SalesInvoiceLineTrans_FactDetail]
-- Inlined methods: SalesinvoicelinetransFact.salesinvoicelinetrans_factstage [SalesInvoiceLineTrans_FactStage], SalesinvoicelinetransFact.salesinvoicelinetrans_factratio [SalesInvoiceLineTrans_FactRatio], SalesinvoicelinetransFact.salesinvoicelinetrans_facttrans [SalesInvoiceLineTrans_FactTrans], SalesinvoicelinetransFact.salesinvoicelinetrans_factadj [SalesInvoiceLineTrans_FactAdj], SalesinvoicelinetransFact.salesinvoicelinetrans_facttransadj [SalesInvoiceLineTrans_FactTransAdj], SalesinvoicelinetransFact.salesinvoicelinetrans_facttransadj2 [SalesInvoiceLineTrans_FactTransAdj2], SalesinvoicelinetransFact.salesinvoicelinetrans_facttransadj3 [SalesInvoiceLineTrans_FactTransAdj3], SalesinvoicelinetransFact.salesinvoicelinetrans_facttransadj4 [SalesInvoiceLineTrans_FactTransAdj4], SalesinvoicelinetransFact.salesinvoicelinetrans_factInvoiceTrans [SalesInvoiceLineTrans_FactInvoiceTrans], SalesinvoicelinetransFact.salesinvoicelinetrans_factInvoiceTrans2 [SalesInvoiceLineTrans_FactInvoiceTrans2]
-- external_table_name: SalesInvoiceLineTrans_FactDetail
-- schema_name: temp

WITH
salesinvoicelinetrans_factstage AS (
    SELECT it.recid                   AS RECID_IT
                , MAX (cit.recid)            AS RECID_CIT
                , MAX (cit.inventqty)         AS InventQty_CIT
                , MAX (cit.itemid)            AS ItemID
                , MAX (cit.qty)               AS Qty_CIT
                , MAX (it.qty * -1)           AS Qty_IT
                , MAX (it.packingslipid)      AS PackingSlipID
                , MAX (cit.modifieddatetime) AS _SourceDate

             FROM {{ ref('custinvoicejour') }}        cij
             LEFT JOIN {{ ref('custinvoicetrans') }}  cit
             ON cij.dataareaid         = cit.dataareaid
             AND cij.salesid             = cit.salesid
             AND cij.invoiceid           = cit.invoiceid
             AND cij.invoicedate         = cit.invoicedate
             AND cij.numbersequencegroup = cit.numbersequencegroup
             AND (cij.recid             = cit.parentrecid OR cij.salestype <> 0)
          INNER JOIN {{ ref('inventtransorigin') }} ito
             ON ito.dataareaid         = cit.dataareaid
             AND ito.inventtransid       = cit.inventtransid
             AND ito.itemid              = cit.itemid
             AND ito.referencecategory   = 0
          INNER JOIN {{ ref('inventtrans') }}       it
             ON it.dataareaid          = ito.dataareaid
             AND it.inventtransorigin    = ito.recid
             AND it.itemid               = ito.itemid
             AND it.invoiceid            = cit.invoiceid
          GROUP BY it.recid;
),
salesinvoicelinetrans_factratio AS (
    SELECT ts.RECID_IT                                                                                           AS RecID_IT
                , CASE WHEN SUM (-1 * ts.Qty_IT) OVER (PARTITION BY ts.RECID_CIT) = 0
                      THEN 1 / CAST(ISNULL (NULLIF(COUNT (1) OVER (PARTITION BY ts.RECID_CIT), 0), 1) AS FLOAT)
                      ELSE
                      CAST(ts.Qty_IT AS FLOAT) * -1
                      / CAST(ISNULL (NULLIF(SUM (-1 * ts.Qty_IT) OVER (PARTITION BY ts.RECID_CIT), 0), 1) AS FLOAT)END AS PercentOfTotal

             FROM salesinvoicelinetrans_factstage ts;
),
salesinvoicelinetrans_facttrans AS (
    SELECT fcl.SalesInvoiceLineKey                                                             AS SalesInvoiceLineKey
                , it.dataareaid                                                                      AS LegalEntityID
                , 1                                                                                   AS InventoryTransStatusID
                , it.itemid                                                                           AS ItemID
                , id.inventbatchid                                                                    AS TagID
                , CAST(fcl.BaseAmount * tr.PercentOfTotal AS MONEY)                                   AS BaseAmount
                , CAST(fcl.BaseAmount_TransCur * tr.PercentOfTotal AS MONEY)                          AS BaseAmount_TransCur
                , CAST((it.qty * -1) * tk.Qty_CIT / ISNULL (NULLIF(tk.InventQty_CIT, 0), 1) AS FLOAT) AS InvoiceQuantity_SalesUOM
                , it.qty * -1                                                                         AS InvoiceQuantity
                , CAST(fcl.DiscountAmount * tr.PercentOfTotal AS MONEY)                               AS DiscountAmount
                , CAST(fcl.DiscountAmount_TransCur * tr.PercentOfTotal AS MONEY)                      AS DiscountAmount_TransCur
                , CAST(fcl.IncludedCharge * tr.PercentOfTotal AS MONEY)                               AS IncludedCharge
                , CAST(fcl.IncludedCharge_TransCur * tr.PercentOfTotal AS MONEY)                      AS IncludedCharge_TransCur
                , CAST(fcl.AdditionalCharge * tr.PercentOfTotal AS MONEY)                             AS AdditionalCharge
                , CAST(fcl.AdditionalCharge_TransCur * tr.PercentOfTotal AS MONEY)                    AS AdditionalCharge_TransCur
                , CAST(fcl.CustomerCharge * tr.PercentOfTotal AS MONEY)                               AS CustomerCharge
                , CAST(fcl.CustomerCharge_TransCur * tr.PercentOfTotal AS MONEY)                      AS CustomerCharge_TransCur
                , CAST(fcl.NonBillableCharge * tr.PercentOfTotal AS MONEY)                            AS NonBillableCharge
                , CAST(fcl.NonBillableCharge_TransCur * tr.PercentOfTotal AS MONEY)                   AS NonBillableCharge_TransCur
                , (it.costamountposted + it.costamountadjustment) * -1                                AS CostAmount
                , (it.costamountposted + it.costamountadjustment) * -100 / cij.exchrate               AS CostAmount_TransCur
                , CAST((fcl.GrossProfit * tr.PercentOfTotal) AS MONEY)                                AS GrossProfit
                , CAST((fcl.GrossProfit_TransCur * tr.PercentOfTotal) AS MONEY)                       AS GrossProfit_TransCur
                , CAST(fcl.InvoiceTotalAmount * tr.PercentOfTotal AS MONEY)                           AS InvoiceTotalAmount
                , CAST(fcl.InvoiceTotalAmount_TransCur * tr.PercentOfTotal AS MONEY)                  AS InvoiceTotalAmount_TransCur
                , CAST(fcl.NetAmount * tr.PercentOfTotal AS NUMERIC(28, 12))                          AS NetAmount
                , CAST(fcl.NetAmount_TransCur * tr.PercentOfTotal AS NUMERIC(28, 12))                 AS NetAmount_TransCur
                , CAST(fcl.InvoiceSalesAmount * tr.PercentOfTotal AS MONEY)                           AS InvoiceSalesAmount
                , CAST(fcl.InvoiceSalesAmount_TransCur * tr.PercentOfTotal AS MONEY)                  AS InvoiceSalesAmount_TransCur
                , CAST(fcl.TaxAmount * tr.PercentOfTotal AS MONEY)                                    AS TaxAmount
                , CAST(fcl.TaxAmount_TransCur * tr.PercentOfTotal AS MONEY)                           AS TaxAmount_TransCur
                , fcl.BaseUnitPrice                                                                   AS BaseUnitPrice
                , fcl.BaseUnitPrice_TransCur                                                          AS BaseUnitPrice_TransCur
                , fcl.TotalUnitPrice                                                                  AS TotalUnitPrice
                , fcl.TotalUnitPrice_TransCur                                                         AS TotalUnitPrice_TransCur
                , fcl.PriceUnit                                                               AS PriceUnit
                , cij.invoicedate                                                                     AS InvoiceDate
                , ISNULL (it.recid, 0)                                                               AS RECID_IT
                , tk.RECID_CIT
                , tk.PackingSlipID                                                                    AS PackingSlipID
                , CASE WHEN ROW_NUMBER () OVER (PARTITION BY fcl.SalesInvoiceLineKey
       ORDER BY ISNULL (it.recid, 0)) = 1
                      THEN 1
                      ELSE 0 END                                                                     AS IsProrateAdj
                , tk._SourceDate

             FROM silver.cma_SalesInvoiceLine_Fact fcl

          INNER JOIN salesinvoicelinetrans_factstage              tk
             ON tk.RECID_CIT            = fcl._RecID2
          INNER JOIN salesinvoicelinetrans_factratio              tr
             ON tr.RecID_IT             = tk.RECID_IT
          INNER JOIN {{ ref('inventtrans') }}      it
             ON it.recid               = tk.RECID_IT
          INNER JOIN {{ ref('inventdim') }}        id
             ON id.dataareaid          = it.dataareaid
             AND id.inventdimid          = it.inventdimid
          INNER JOIN {{ ref('custinvoicetrans') }} cit
             ON cit.recid              = fcl._RecID2
          INNER JOIN {{ ref('custinvoicejour') }}  cij
             ON cij.dataareaid        = cit.dataareaid
             AND cij.salesid             = cit.salesid
             AND cij.invoiceid           = cit.invoiceid
             AND cij.invoicedate         = cit.invoicedate
             AND cij.numbersequencegroup = cit.numbersequencegroup
             AND (cij.recid            = cit.parentrecid OR cij.salestype <> 0)
          WHERE fcl._SourceID = 1;
),
salesinvoicelinetrans_factadj AS (
    SELECT t.SalesInvoiceLineKey
                , t.RECID_IT
                , t.RECID_CIT
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.BaseAmount - SUM (t.BaseAmount) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS BaseAmountAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.BaseAmount_TransCur
                               - SUM (t.BaseAmount_TransCur) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS BaseAmount_TransCurAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.DiscountAmount - SUM (t.DiscountAmount) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS DiscountAmountAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.DiscountAmount_TransCur
                               - SUM (t.DiscountAmount_TransCur) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS DiscountAmount_TransCurAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.IncludedCharge - SUM (t.IncludedCharge) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS IncludedChargeAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.IncludedCharge_TransCur
                               - SUM (t.IncludedCharge_TransCur) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS IncludedCharge_TransCurAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.AdditionalCharge - SUM (t.AdditionalCharge) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS AdditionalChargeAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.AdditionalCharge_TransCur
                               - SUM (t.AdditionalCharge_TransCur) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS AdditionalCharge_TransCurAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.CustomerCharge - SUM (t.CustomerCharge) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS CustomerChargeAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.CustomerCharge_TransCur
                               - SUM (t.CustomerCharge_TransCur) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS CustomerCharge_TransCurAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.NonBillableCharge - SUM (t.NonBillableCharge) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS NonBillableChargeAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.NonBillableCharge_TransCur
                               - SUM (t.NonBillableCharge_TransCur) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS NonBillableCharge_TransCurAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.GrossProfit - SUM (t.GrossProfit) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS GrossProfitAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.GrossProfit_TransCur
                               - SUM (t.GrossProfit_TransCur) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS GrossProfit_TransCurAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.InvoiceTotalAmount - SUM (t.InvoiceTotalAmount) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS InvoiceTotalAmountAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.InvoiceTotalAmount_TransCur
                               - SUM (t.InvoiceTotalAmount_TransCur) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS InvoiceTotalAmount_TransCurAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.NetAmount - SUM (t.NetAmount) OVER (PARTITION BY t.SalesInvoiceLineKey) ELSE 0 END AS NUMERIC(28, 12)) AS NetAmountAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.NetAmount_TransCur - SUM (t.NetAmount_TransCur) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS NUMERIC(28, 12))                                                                                  AS NetAmount_TransCurAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.InvoiceSalesAmount - SUM (t.InvoiceSalesAmount) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS InvoiceSalesAmountAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.InvoiceSalesAmount_TransCur
                               - SUM (t.InvoiceSalesAmount_TransCur) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS InvoiceSalesAmount_TransCurAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.TaxAmount - SUM (t.TaxAmount) OVER (PARTITION BY t.SalesInvoiceLineKey) ELSE 0 END AS MONEY)           AS TaxAmountAdj
                , CAST(CASE WHEN t.IsProrateAdj = 1
                            THEN fcl.TaxAmount_TransCur - SUM (t.TaxAmount_TransCur) OVER (PARTITION BY t.SalesInvoiceLineKey)
                            ELSE 0 END AS MONEY)                                                                                            AS TaxAmount_TransCurAdj

             FROM salesinvoicelinetrans_facttrans                        t
          INNER JOIN silver.cma_SalesInvoiceLine_Fact fcl
             ON fcl.SalesInvoiceLineKey = t.SalesInvoiceLineKey
),
salesinvoicelinetrans_facttransadj AS (
    SELECT SalesInvoiceLineKey
                , RECID_IT
                , RECID_CIT
                , BaseAmountAdj
                , BaseAmount_TransCurAdj
                , DiscountAmountAdj
                , DiscountAmount_TransCurAdj
                , IncludedChargeAdj
                , IncludedCharge_TransCurAdj
                , AdditionalChargeAdj
                , AdditionalCharge_TransCurAdj
                , CustomerChargeAdj
                , CustomerCharge_TransCurAdj
                , NonBillableChargeAdj
                , NonBillableCharge_TransCurAdj
                , GrossProfitAdj
                , GrossProfit_TransCurAdj
                , InvoiceTotalAmountAdj
                , InvoiceTotalAmount_TransCurAdj
                , NetAmountAdj
                , NetAmount_TransCurAdj
                , InvoiceSalesAmountAdj
                , InvoiceSalesAmount_TransCurAdj
                , TaxAmountAdj
                , TaxAmount_TransCurAdj

             FROM salesinvoicelinetrans_factadj
          WHERE BaseAmountAdj                  <> 0
             OR BaseAmount_TransCurAdj         <> 0
             OR DiscountAmountAdj              <> 0
             OR DiscountAmount_TransCurAdj     <> 0
             OR IncludedChargeAdj              <> 0
             OR IncludedCharge_TransCurAdj     <> 0
             OR AdditionalChargeAdj            <> 0
             OR AdditionalCharge_TransCurAdj   <> 0
             OR CustomerChargeAdj              <> 0
             OR CustomerCharge_TransCurAdj     <> 0
             OR NonBillableChargeAdj           <> 0
             OR NonBillableCharge_TransCurAdj  <> 0
             OR GrossProfitAdj                 <> 0
             OR GrossProfit_TransCurAdj        <> 0
             OR InvoiceTotalAmountAdj          <> 0
             OR InvoiceTotalAmount_TransCurAdj <> 0
             OR NetAmountAdj                   <> 0
             OR NetAmount_TransCurAdj          <> 0
             OR InvoiceSalesAmountAdj          <> 0
             OR InvoiceSalesAmount_TransCurAdj <> 0
             OR TaxAmountAdj                   <> 0
             OR TaxAmount_TransCurAdj          <> 0;
),
salesinvoicelinetrans_facttransadj2 AS (
    SELECT t.SalesInvoiceLineKey                                                             AS SalesInvoiceLineKey
                , t.LegalEntityID
                , 1                                                                                   AS InventoryTransStatusID
                , t.ITEMID                                                                           AS ItemID
                , t.TagID
                , t.CostAmount
                , t.CostAmount_TransCur
                , t. InvoiceQuantity_SalesUOM
                , t.InvoiceQuantity
                ,t.BaseAmount + ta.BaseAmountAdj          AS                           BaseAmount
                , t.BaseAmount_TransCur + ta.BaseAmount_TransCurAdj AS                   BaseAmount_TransCur
                , t.DiscountAmount + ta.DiscountAmountAdj AS                              DiscountAmount
                , t.DiscountAmount_TransCur + ta.DiscountAmount_TransCurAdj  AS             DiscountAmount_TransCur
                , t.IncludedCharge + ta.IncludedChargeAdj                      AS          IncludedCharge
                , t.IncludedCharge_TransCur + ta.IncludedCharge_TransCurAdj      AS        IncludedCharge_TransCur
                , t.AdditionalCharge + ta.AdditionalChargeAdj                      AS       AdditionalCharge
                , t.AdditionalCharge_TransCur + ta.AdditionalCharge_TransCurAdj      AS     AdditionalCharge_TransCur    
                , t.CustomerCharge + ta.CustomerChargeAdj                              AS   CustomerCharge
                , t.CustomerCharge_TransCur + ta.CustomerCharge_TransCurAdj  AS CustomerCharge_TransCur
                ,t.NonBillableCharge + ta.NonBillableChargeAdj AS              NonBillableCharge
                ,t.NonBillableCharge_TransCur + ta.NonBillableCharge_TransCurAdj AS  NonBillableCharge_TransCur
                ,t.GrossProfit + ta.GrossProfitAdj      AS    GrossProfit
                ,t.GrossProfit_TransCur + ta.GrossProfit_TransCurAdj AS   GrossProfit_TransCur
                ,t.InvoiceTotalAmount + ta.InvoiceTotalAmountAdj  AS      InvoiceTotalAmount
                ,t.InvoiceTotalAmount_TransCur + ta.InvoiceTotalAmount_TransCurAdj AS  InvoiceTotalAmount_TransCur
                ,t.NetAmount + ta.NetAmountAdj   AS  NetAmount
                ,t.NetAmount_TransCur + ta.NetAmount_TransCurAdj  AS   NetAmount_TransCur
                ,t.InvoiceSalesAmount + ta.InvoiceSalesAmountAdj  AS   InvoiceSalesAmount
                ,t.InvoiceSalesAmount_TransCur + ta.InvoiceSalesAmount_TransCurAdj AS  InvoiceSalesAmount_TransCur 
                ,t.TaxAmount + ta.TaxAmountAdj AS   TaxAmount
                ,t.TaxAmount_TransCur + ta.TaxAmount_TransCurAdj AS  TaxAmount_TransCur
                , t.BaseUnitPrice                                                                   AS BaseUnitPrice
                , t.BaseUnitPrice_TransCur                                                          AS BaseUnitPrice_TransCur
                , t.TotalUnitPrice                                                                  AS TotalUnitPrice
                , t.TotalUnitPrice_TransCur                                                         AS TotalUnitPrice_TransCur
                , t.PriceUnit                                                               AS PriceUnit
                , t.INVOICEDATE                                                                     AS InvoiceDate
                , t. RECID_IT
                , t.RECID_CIT
                , t.PackingSlipID                                                                    AS PackingSlipID
                ,t.IsProrateAdj
                , t._SourceDate
             FROM salesinvoicelinetrans_facttrans         t
          INNER JOIN salesinvoicelinetrans_facttransadj ta
             ON ta.SalesInvoiceLineKey = t.SalesInvoiceLineKey
             AND ta.RECID_IT            = t.RECID_IT;
),
salesinvoicelinetrans_facttransadj3 AS (
    SELECT t. SalesInvoiceLineKey
                , t.LegalEntityID
                , t.InventoryTransStatusID
                , t.ItemID
                , t.TagID
                , t.CostAmount
                , t.CostAmount_TransCur
                , t.InvoiceQuantity_SalesUOM
                , t.InvoiceQuantity
                ,t.BaseAmount
                , t.BaseAmount_TransCur
                , t.DiscountAmount
                , t.DiscountAmount_TransCur
                , t.IncludedCharge
                , t.IncludedCharge_TransCur
                , t.AdditionalCharge
                , t.AdditionalCharge_TransCur    
                , t.CustomerCharge
                , t.CustomerCharge_TransCur
                ,t.NonBillableCharge
                ,t.NonBillableCharge_TransCur
                ,t.GrossProfit
                ,t.GrossProfit_TransCur
                ,t.InvoiceTotalAmount
                ,t.InvoiceTotalAmount_TransCur
                ,t.NetAmount
                ,t.NetAmount_TransCur
                ,t.InvoiceSalesAmount
                ,t.InvoiceSalesAmount_TransCur 
                ,t.TaxAmount
                ,t.TaxAmount_TransCur
                , t. BaseUnitPrice
                , t.BaseUnitPrice_TransCur
                , t.TotalUnitPrice
                , t.TotalUnitPrice_TransCur
                , t.PriceUnit
                ,t.InvoiceDate
                , t. RECID_IT
                , t.RECID_CIT
                , t.PackingSlipID
                ,t.IsProrateAdj
                , t._SourceDate
             FROM salesinvoicelinetrans_facttrans         t
             LEFT JOIN salesinvoicelinetrans_facttransadj  ta
             ON ta.SalesInvoiceLineKey = t.SalesInvoiceLineKey
             AND ta.RECID_IT            = t.RECID_IT
             WHERE  ta.SalesInvoiceLineKey IS NULL
),
salesinvoicelinetrans_facttransadj4 AS (
    Select * from   salesinvoicelinetrans_facttransadj2
       UNION ALL
       Select * from   salesinvoicelinetrans_facttransadj3
),
salesinvoicelinetrans_factinvoicetrans AS (
    SELECT frl.SalesInvoiceLineKey
       , polt.SalesOrderLineTransKey
       , ROW_NUMBER () OVER (PARTITION BY frl.SalesInvoiceLineKey
    ORDER BY polt._RecID2) AS OrderTransRank
    FROM silver.cma_SalesInvoiceLine_Fact         frl
    INNER JOIN silver.cma_SalesOrderLineTrans_Fact polt
       ON polt.SalesOrderLineKey = frl.SalesOrderLineKey;
),
salesinvoicelinetrans_factinvoicetrans2 AS (
    SELECT frl.SalesInvoiceLineKey
       , prlt.PackingSlipLineTransKey
       , prlt.SalesOrderLineTransKey
       , ROW_NUMBER () OVER (PARTITION BY frl.SalesInvoiceLineKey
    ORDER BY prlt._RecID2) AS OrderTransRank
    FROM silver.cma_SalesInvoiceLine_Fact          frl
    INNER JOIN silver.cma_PackingSlipLine_Fact      prl
       ON prl.SalesOrderLineKey   = frl.SalesOrderLineKey
    INNER JOIN silver.cma_PackingSlipLineTrans_Fact prlt
       ON prlt.PackingSlipLineKey = prl.PackingSlipLineKey;
)
SELECT ROW_NUMBER() OVER (ORDER BY tt.PackingSlipID) AS SalesInvoiceLineTransKey
         ,fcl.SalesInvoiceLineKey                                                                                AS SalesInvoiceLineKey
               , ISNULL (s.InventoryTransStatusKey, -1)                                                                 AS InventoryTransStatusKey
               , dt.TagKey                                                                                              AS TagKey
               , COALESCE (pslt.PackingSlipLineTransKey, it2.PackingSlipLineTransKey, -1)                               AS PackingSlipLineTransKey	
               , COALESCE (pslt.SalesOrderLineTransKey, it1.SalesOrderLineTransKey, it2.SalesOrderLineTransKey, -1)     AS SalesOrderLineTransKey
               , ISNULL (tt.AdditionalCharge, fcl.AdditionalCharge)                                                     AS AdditionalCharge
               , ISNULL (tt.AdditionalCharge_TransCur, fcl.AdditionalCharge_TransCur)                                   AS AdditionalCharge_TransCur
               , ISNULL (tt.BaseAmount, fcl.BaseAmount)                                                                 AS BaseAmount
               , ISNULL (tt.BaseAmount_TransCur, fcl.BaseAmount_TransCur)                                               AS BaseAmount_TransCur
               , fcl.BaseUnitPrice                                                                                      AS BaseUnitPrice
               , fcl.BaseUnitPrice_TransCur                                                                             AS BaseUnitPrice_TransCur
               , ISNULL (tt.CostAmount, fcl.CostAmount)                                                                 AS CostAmount
               , ISNULL (tt.CostAmount_TransCur, fcl.CostAmount_TransCur)                                               AS CostAmount_TransCur
               , ISNULL (tt.CustomerCharge, fcl.CustomerCharge)                                                         AS CustomerCharge
               , ISNULL (tt.CustomerCharge_TransCur, fcl.CustomerCharge_TransCur)                                       AS CustomerCharge_TransCur
               , ISNULL (tt.DiscountAmount, fcl.DiscountAmount)                                                         AS DiscountAmount
               , ISNULL (tt.DiscountAmount_TransCur, fcl.DiscountAmount_TransCur)                                       AS DiscountAmount_TransCur
               , ISNULL (tt.GrossProfit, fcl.GrossProfit)                                                               AS GrossProfit
               , ISNULL (tt.GrossProfit_TransCur, fcl.GrossProfit_TransCur)                                             AS GrossProfit_TransCur
               , ISNULL (tt.IncludedCharge, fcl.IncludedCharge)                                                         AS IncludedCharge
               , ISNULL (tt.IncludedCharge_TransCur, fcl.IncludedCharge_TransCur)                                       AS IncludedCharge_TransCur
               , ISNULL (tt.InvoiceTotalAmount, fcl.InvoiceTotalAmount)                                                 AS InvoiceTotalAmount
               , ISNULL (tt.InvoiceTotalAmount_TransCur, fcl.InvoiceTotalAmount_TransCur)                               AS InvoiceTotalAmount_TransCur
               , ISNULL (tt.InvoiceQuantity, fcl.InvoiceQuantity)                                                       AS InvoiceQuantity
               , ISNULL (tt.InvoiceQuantity, 1) * fcl.InvoiceQuantity_LB / ISNULL (NULLIF(fcl.InvoiceQuantity, 0), 1)   AS InvoiceQuantity_LB
               , ISNULL (tt.InvoiceQuantity, 1) * fcl.InvoiceQuantity_PC / ISNULL (NULLIF(fcl.InvoiceQuantity, 0), 1)   AS InvoiceQuantity_PC

               , ISNULL (tt.InvoiceQuantity, 1) * fcl.InvoiceQuantity_FT / ISNULL (NULLIF(fcl.InvoiceQuantity, 0), 1)   AS InvoiceQuantity_FT

               , ISNULL (tt.InvoiceQuantity, 1) * fcl.InvoiceQuantity_SQIN / ISNULL (NULLIF(fcl.InvoiceQuantity, 0), 1) AS InvoiceQuantity_SQIN
               , ISNULL (tt.InvoiceQuantity_SalesUOM, fcl.InvoiceQuantity_SalesUOM)                                     AS InvoiceQuantity_SalesUOM
               , ISNULL (tt.NetAmount, fcl.NetAmount)                                                                   AS NetAmount
               , ISNULL (tt.NetAmount_TransCur, fcl.NetAmount_TransCur)                                                 AS NetAmount_TransCur
               , ISNULL (tt.NonBillableCharge, fcl.NonBillableCharge)                                                   AS NonBillableCharge
               , ISNULL (tt.NonBillableCharge_TransCur, fcl.NonBillableCharge_TransCur)                                 AS NonBillableCharge_TransCur
               , fcl.PriceUnit                                                                                          AS PriceUnit
               , ISNULL (tt.TaxAmount, fcl.TaxAmount)                                                                   AS TaxAmount
               , ISNULL (tt.TaxAmount_TransCur, fcl.TaxAmount_TransCur)                                                 AS TaxAmount_TransCur
               , ISNULL (tt.InvoiceSalesAmount, fcl.InvoiceSalesAmount)                                                 AS InvoiceSalesAmount
               , ISNULL (tt.InvoiceSalesAmount_TransCur, fcl.InvoiceSalesAmount_TransCur)                               AS InvoiceSalesAmount_TransCur
               , fcl.TotalUnitPrice                                                                                     AS TotalUnitPrice
               , fcl.TotalUnitPrice_TransCur                                                                            AS TotalUnitPrice_TransCur
               , tt.PackingSlipID                                                                                       AS PackingSlipID
               , tt._SourceDate
               , ISNULL (tt.RECID_IT, 0)                                                                                AS _RecID3
               , fcl._RecID2                                                                                            AS _RECID2
               , fcl._RecID1                                                                                            AS _RECID1
               , 1                                                                                                      AS _SourceID
               ,  CURRENT_TIMESTAMP  AS  _CreatedDate
               , CURRENT_TIMESTAMP AS _ModifiedDate

            FROM silver.cma_SalesInvoiceLine_Fact          fcl

            LEFT JOIN  salesinvoicelinetrans_facttransadj4                       tt
            ON tt.SalesInvoiceLineKey       = fcl.SalesInvoiceLineKey
            LEFT JOIN silver.cma_Tag                       dt
            ON dt.LegalEntityID             = tt.LegalEntityID
            AND dt.TagID                     = tt.TagID
            AND dt.ItemID                    = tt.ItemID
            LEFT JOIN silver.cma_PackingSlipLineTrans_Fact pslt
            ON pslt._RecID2                 = tt.RECID_IT
            AND pslt._SourceID               = 1
            LEFT JOIN {{ ref('inventtrans') }}               it
            ON it.recid                   = tt.RECID_IT
            LEFT JOIN silver.cma_InventoryTransStatus      s
            ON s.InventoryTransStatusID     = tt.InventoryTransStatusID
            AND s.InventoryTransStatusTypeID = 1
            LEFT JOIN salesinvoicelinetrans_factinvoicetrans                it1	
            ON it1.SalesInvoiceLineKey      = fcl.SalesInvoiceLineKey	
            AND it1.OrderTransRank           = 1	
            LEFT JOIN  salesinvoicelinetrans_factinvoicetrans2           it2	
            ON it2.SalesInvoiceLineKey      = fcl.SalesInvoiceLineKey	
            AND it2.OrderTransRank           = 1;
