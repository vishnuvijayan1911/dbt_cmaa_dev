{{ config(materialized='table', tags=['silver'], alias='salesinvoicelinetaxtrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/salesinvoicelinetaxtrans_f/salesinvoicelinetaxtrans_f.py
-- Root method: SalesinvoicelinetaxtransFact.salesinvoicelinetaxtrans_factdetail [SalesInvoiceLineTaxTrans_FactDetail]
-- Inlined methods: SalesinvoicelinetaxtransFact.salesinvoicelinetaxtrans_factsilkeys [SalesInvoiceLineTaxTrans_FactSILKeys], SalesinvoicelinetaxtransFact.salesinvoicelinetaxtrans_factsilratio [SalesInvoiceLineTaxTrans_FactSILRatio]
-- external_table_name: SalesInvoiceLineTaxTrans_FactDetail
-- schema_name: temp

WITH
salesinvoicelinetaxtrans_factsilkeys AS (
    SELECT it.recid            AS RecID_IT
             , MAX(cit.recid)      AS RecID_CIT
             , MAX(cit.inventqty)   AS InventQty_CIT
             , MAX(cit.itemid)      AS ItemID
             , it.qty               AS Qty_IT
             , tt.recid            AS RecID_TT
             , MAX(tt.currencycode) AS TaxCurrencyID
             , MAX(tt.dataareaid)  AS LegalEntityID

          FROM {{ ref('custinvoicejour') }}        cij
         INNER JOIN {{ ref('custinvoicetrans') }}  cit
            ON cij.dataareaid         = cit.dataareaid
           AND cij.salesid             = cit.salesid
           AND cij.invoiceid           = cit.invoiceid
           AND cij.invoicedate         = cit.invoicedate
           AND cij.numbersequencegroup = cit.numbersequencegroup
           AND (cij.recid              = cit.parentrecid OR cij.salestype <> 0)
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid        = cit.dataareaid
           AND ito.inventtransid       = cit.inventtransid
           AND ito.itemid              = cit.itemid
           AND ito.referencecategory   = 0
         INNER JOIN {{ ref('inventtrans') }}       it
            ON it.dataareaid          = ito.dataareaid
           AND it.inventtransorigin    = ito.recid
           AND it.itemid               = ito.itemid
           AND it.invoiceid            = cit.invoiceid
          LEFT JOIN {{ ref('taxtrans') }}          tt
            ON tt.sourcerecid          = cit.recid



         GROUP BY it.recid
                , it.qty
                , tt.recid;
),
salesinvoicelinetaxtrans_factsilratio AS (
    SELECT tk.RecID_IT
             , tk.RecID_TT
             , CASE WHEN tk.InventQty_CIT = 0
                    THEN 1 / CAST(ISNULL(NULLIF(COUNT(*) OVER (PARTITION BY tk.RecID_CIT), 0), 1) AS FLOAT)
                    ELSE CAST(tk.Qty_IT AS FLOAT) * -1 / CAST(ISNULL(NULLIF(tk.InventQty_CIT, 0), 1) AS FLOAT)END AS PercentOfTotal

          FROM salesinvoicelinetaxtrans_factsilkeys tk;
)
SELECT 
           CURRENT_TIMESTAMP                                                                      AS _CreatedDate
         , CURRENT_TIMESTAMP                                                                      AS _ModifiedDate 
         , ROW_NUMBER() OVER (ORDER BY t._RecID1) AS SalesInvoiceLineTaxTransKey
         , * FROM ( SELECT DISTINCT
           fclt.SalesInvoiceLineTransKey
         , ft.SalesInvoiceLineTaxKey
         , ft.LegalEntityKey
         , ft.TransDateKey
         , ft.SalesInvoiceLineKey
         , ft.TaxGroupKey
         , ft.TaxCodeKey
         , ft.TaxCurrencyKey
         , ft.TaxAmount_TransCur * tr.PercentOfTotal AS TaxAmount_TransCur
         , ft.TaxAmount * tr.PercentOfTotal          AS TaxAmount
         , tk.RecID_IT                               AS _RecID1
         , tk.RecID_TT                               AS _RecID2
         , 1                                         AS _SourceID

       FROM {{ ref('salesinvoicelinetax_f') }}        ft
     INNER JOIN salesinvoicelinetaxtrans_factsilkeys                       tk
        ON tk.RecID_TT = ft._RecID
     INNER JOIN salesinvoicelinetaxtrans_factsilratio                    tr
        ON tr.RecID_IT = tk.RecID_IT
       AND tr.RecID_TT = tk.RecID_TT
      LEFT JOIN {{ ref('salesinvoicelinetrans_f') }} fclt
        ON fclt._RecID3 = tr.RecID_IT) t;
