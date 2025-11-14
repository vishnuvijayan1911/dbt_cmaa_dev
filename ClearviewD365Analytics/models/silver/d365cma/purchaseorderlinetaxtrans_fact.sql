{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/purchaseorderlinetaxtrans_fact/purchaseorderlinetaxtrans_fact.py
-- Root method: PurchaseorderlinetaxtransFact.purchaseorderlinetaxtrans_factdetail [PurchaseOrderLineTaxTrans_FactDetail]
-- Inlined methods: PurchaseorderlinetaxtransFact.purchaseorderlinetaxtrans_factpolkeys [PurchaseOrderLineTaxTrans_FactPOLKeys], PurchaseorderlinetaxtransFact.purchaseorderlinetaxtrans_factpolratio [PurchaseOrderLineTaxTrans_FactPOLRatio]
-- external_table_name: PurchaseOrderLineTaxTrans_FactDetail
-- schema_name: temp

WITH
purchaseorderlinetaxtrans_factpolkeys AS (
    SELECT DISTINCT
               it.recid       AS RecID_IT
             , pl.recid       AS RecID_PL
             , pl.qtyordered   AS OrderQty_PL
             , it.qty          AS Qty_IT
             , pl.itemid
             , tt.recid       AS RecID_TT
             , tt.currencycode AS TaxCurrencyID
             , tt.dataareaid  AS LegalEntityID

          FROM {{ ref('purchtable') }}             pt
         INNER JOIN {{ ref('purchline') }}         pl
            ON pl.dataareaid       = pt.dataareaid
           AND pl.purchid           = pt.purchid
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid      = pl.dataareaid
           AND ito.inventtransid    = pl.inventtransid
           AND ito.itemid           = pl.itemid
         INNER JOIN {{ ref('inventtrans') }}       it
            ON it.inventtransorigin = ito.recid
           AND it.dataareaid       = ito.dataareaid
           AND it.statusreceipt IN ( 1, 2, 3, 4, 5 )
          LEFT JOIN {{ ref('vendinvoicetrans') }}  vit
            ON vit.dataareaid      = pt.dataareaid
          LEFT JOIN {{ ref('taxtrans') }}          tt
            ON pl.recid            = tt.sourcerecid
         INNER JOIN {{ ref('sqldictionary') }}     sd
            ON sd.tabid             = tt.sourcetableid
           AND sd.name              = 'PurchLine';
),
purchaseorderlinetaxtrans_factpolratio AS (
    SELECT tk.RecID_IT
             , tk.RecID_TT
             , CASE WHEN tk.OrderQty_PL = 0
                    THEN 1 / CAST(ISNULL(NULLIF(COUNT(*) OVER (PARTITION BY tk.RecID_PL), 0), 1) AS FLOAT)
                    ELSE CAST(tk.Qty_IT AS FLOAT) * -1 / CAST(ISNULL(NULLIF(tk.OrderQty_PL, 0), 1) AS FLOAT)END AS PercentOfTotal

          FROM purchaseorderlinetaxtrans_factpolkeys tk;
)
SELECT DISTINCT
           CURRENT_TIMESTAMP                                                                      AS _CreatedDate
         , CURRENT_TIMESTAMP                                                                      AS _ModifiedDate 
         , ROW_NUMBER() OVER (ORDER BY tk.RecID_IT) AS PurchaseOrderLineTaxTransKey
          , ft.PurchaseOrderLineKey
         , ft.PurchaseOrderLineTaxKey
         , fclt.PurchaseOrderLineTransKey
         , ft.LegalEntityKey
         , ft.TaxCodeKey
         , ft.TaxCurrencyKey
         , ft.TaxGroupKey
         , ft.TransDateKey
         , ft.TaxAmount_TransCur * tr.PercentOfTotal AS TaxAmount_TransCur
         , ft.TaxAmount * tr.PercentOfTotal          AS TaxAmount
         , tk.RecID_IT                               AS _RecID1
         , tk.RecID_TT                               AS _RecID2
         , 1                                         AS _SourceID 

      FROM silver.cma_PurchaseOrderLineTax_Fact        ft
     INNER JOIN purchaseorderlinetaxtrans_factpolkeys                        tk
        ON tk.RecID_TT = ft._RecID
     INNER JOIN purchaseorderlinetaxtrans_factpolratio                       tr
        ON tr.RecID_IT = tk.RecID_IT
       AND tr.RecID_TT = tk.RecID_TT
      LEFT JOIN silver.cma_PurchaseOrderLineTrans_Fact fclt
        ON fclt._RecID1 = tr.RecID_IT;
