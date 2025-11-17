{{ config(materialized='table', tags=['silver'], alias='purchaseinvoicelinetaxtrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseinvoicelinetaxtrans_f/purchaseinvoicelinetaxtrans_f.py
-- Root method: PurchaseinvoicelinetaxtransFact.purchaseinvoicelinetaxtrans_factdetail [PurchaseInvoiceLineTaxTrans_FactDetail]
-- Inlined methods: PurchaseinvoicelinetaxtransFact.purchaseinvoicelinetaxtrans_factpilkeys [PurchaseInvoiceLineTaxTrans_FactPILKeys], PurchaseinvoicelinetaxtransFact.purchaseinvoicelinetaxtrans_factpilratio [PurchaseInvoiceLineTaxTrans_FactPILRatio]
-- external_table_name: PurchaseInvoiceLineTaxTrans_FactDetail
-- schema_name: temp

WITH
purchaseinvoicelinetaxtrans_factpilkeys AS (
    SELECT it.recid          AS RecID_IT
             , MAX(vit.recid)    AS RecID_VIT
             , MAX(vij.recid)    AS RecID_VIJ
             , MAX(vit.inventqty) AS InventoryQty_VIT
             , MAX(vit.itemid)    AS ItemID
             , MAX(vit.qty)       AS Qty_VIT
             , it.qty             AS Qty_IT
             , tt.recid          AS RecID_TT
             , tt.currencycode    AS TaxCurrencyID
             , tt.dataareaid     AS LegalEntityID

          FROM {{ ref('vendinvoicejour') }}        vij
         INNER JOIN {{ ref('vendinvoicetrans') }}  vit
            ON vij.dataareaid         = vit.dataareaid
           AND vij.purchid             = vit.purchid
           AND vij.invoiceid           = vit.invoiceid
           AND vij.invoicedate         = vit.invoicedate
           AND vij.numbersequencegroup = vit.numbersequencegroup
           AND vij.internalinvoiceid   = vit.internalinvoiceid
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
         INNER JOIN {{ ref('taxtrans') }}          tt
            ON vit.dataareaid         = tt.dataareaid
           AND vit.recid              = tt.sourcerecid
         INNER JOIN {{ ref('sqldictionary') }}     sd
            ON sd.tabid                = tt.sourcetableid
           AND sd.name                 = 'VendInvoiceTrans'
         GROUP BY it.recid
                , it.qty
                , tt.recid
                , tt.currencycode
                , tt.dataareaid;
),
purchaseinvoicelinetaxtrans_factpilratio AS (
    SELECT tk.RecID_IT
             , tk.RecID_TT
             , CASE WHEN tk.InventoryQty_VIT = 0
                    THEN 1 / CAST(ISNULL(NULLIF(COUNT(*) OVER (PARTITION BY tk.RecID_VIT), 0), 1) AS FLOAT)
                    ELSE CAST(tk.Qty_IT AS FLOAT) / CAST(ISNULL(NULLIF(tk.InventoryQty_VIT, 0), 1) AS FLOAT)END AS PercentOfTotal

          FROM purchaseinvoicelinetaxtrans_factpilkeys tk;
)
SELECT 
           CURRENT_TIMESTAMP                                                                      AS _CreatedDate
         , CURRENT_TIMESTAMP                                                                      AS _ModifiedDate 
         , ROW_NUMBER() OVER (ORDER BY t._RecID) AS PurchaseInvoiceLineTaxTransKey
         , * FROM ( SELECT DISTINCT
          ft.PurchaseInvoiceLineKey
         , ft.PurchaseInvoiceLineTaxKey
         , fclt.PurchaseInvoiceLineTransKey
         , ft.LegalEntityKey
         , ft.TaxCodeKey
         , ft.TaxCurrencyKey
         , ft.TaxGroupKey
         , ft.TransDateKey
         , ft.TaxAmount * tr.PercentOfTotal          AS TaxAmount
         , ft.TaxAmount_TransCur * tr.PercentOfTotal AS TaxAmount_TransCur
         , tk.RecID_IT                               AS _RecID
         , tk.RecID_TT                               AS _RecID2
         , 1                                         AS _SourceID

      FROM {{ ref('purchaseinvoicelinetax_f') }}        ft
     INNER JOIN purchaseinvoicelinetaxtrans_factpilkeys                          tk
        ON tk.RecID_TT  = ft._RecID
     INNER JOIN purchaseinvoicelinetaxtrans_factpilratio                         tr
        ON tr.RecID_IT  = tk.RecID_IT
       AND tr.RecID_TT  = tk.RecID_TT
      LEFT JOIN {{ ref('purchaseinvoicelinetrans_f') }} fclt
        ON fclt._RecID3 = tr.RecID_IT ) t;
