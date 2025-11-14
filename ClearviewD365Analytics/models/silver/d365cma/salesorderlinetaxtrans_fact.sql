{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/salesorderlinetaxtrans_fact/salesorderlinetaxtrans_fact.py
-- Root method: SalesorderlinetaxtransFact.salesorderlinetaxtrans_factdetail [SalesOrderLineTaxTrans_FactDetail]
-- Inlined methods: SalesorderlinetaxtransFact.salesorderlinetaxtrans_factsolkeys [SalesOrderLineTaxTrans_FactSOLKeys], SalesorderlinetaxtransFact.salesorderlinetaxtrans_factsolratio [SalesOrderLineTaxTrans_FactSOLRatio]
-- external_table_name: SalesOrderLineTaxTrans_FactDetail
-- schema_name: temp

WITH
salesorderlinetaxtrans_factsolkeys AS (
    SELECT DISTINCT
               it.recid       AS RecID_IT
             , sl.recid       AS RecID_SL
             , sl.itemid
             , sl.qtyordered   AS OrderQty_SL
             , it.qty          AS Qty_IT
             , sl.currencycode
             , tt.recid       AS RecID_TT
             , tt.currencycode AS TaxCurrencyID
             , tt.dataareaid  AS LegalEntityID

          FROM  {{ ref('salestable') }}         st
         INNER JOIN {{ ref('salesline') }}         sl
            ON sl.dataareaid       = st.dataareaid
           AND sl.salesid           = st.salesid
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid      = sl.dataareaid
           AND ito.inventtransid    = sl.inventtransid
           AND ito.itemid           = sl.itemid
         INNER JOIN {{ ref('inventtrans') }}       it
            ON it.inventtransorigin = ito.recid
           AND it.dataareaid       = ito.dataareaid
           AND it.statusissue IN ( 1, 2, 3, 4, 5, 6 )
         INNER JOIN {{ ref('taxtrans') }}          tt
            ON sl.dataareaid       = tt.dataareaid
           AND sl.recid            = tt.sourcerecid
         INNER JOIN {{ ref('sqldictionary') }}     sd
            ON sd.tabid             = tt.sourcetableid
           AND sd.name              = 'SalesLine';
),
salesorderlinetaxtrans_factsolratio AS (
    SELECT tk.RecID_IT
             , tk.RecID_TT
             , CASE WHEN tk.OrderQty_SL = 0
                    THEN 1 / CAST(ISNULL(NULLIF(COUNT(*) OVER (PARTITION BY tk.RecID_SL), 0), 1) AS FLOAT)
                    ELSE CAST(tk.Qty_IT AS FLOAT) * -1 / CAST(ISNULL(NULLIF(tk.OrderQty_SL, 0), 1) AS FLOAT)END AS PercentOfTotal

          FROM salesorderlinetaxtrans_factsolkeys tk;
)
SELECT DISTINCT
           CURRENT_TIMESTAMP                                                                      AS _CreatedDate
         , CURRENT_TIMESTAMP                                                                      AS _ModifiedDate 
         , ft.SalesOrderLineTaxKey
         , fclt.SalesOrderLineTransKey
         , ft.SalesOrderLineKey
         , ft.LegalEntityKey
         , ft.TransDateKey
         , ft.TaxGroupKey
         , ft.TaxCodeKey
         , ft.TaxCurrencyKey
         , ft.TaxAmount * tr.PercentOfTotal          AS TaxAmount
         , ft.TaxAmount_TransCur * tr.PercentOfTotal AS TaxAmount_TransCur
         , tk.RecID_IT                               AS _RecID1
         , tk.RecID_TT                               AS _RecID2
         , 1                                         AS _SourceID

      FROM silver.cma_SalesOrderLineTax_Fact           ft
     INNER JOIN salesorderlinetaxtrans_factsolkeys                        tk
        ON tk.RecID_TT = ft._RecID
     INNER JOIN salesorderlinetaxtrans_factsolratio                       tr
        ON tr.RecID_IT = tk.RecID_IT
       AND tr.RecID_TT = tk.RecID_TT
      LEFT JOIN silver.cma_SalesOrderLineTrans_Fact fclt
        ON fclt._RecID1 = tr.RecID_IT;
