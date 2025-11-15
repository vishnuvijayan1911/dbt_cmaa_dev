{{ config(materialized='table', tags=['silver'], alias='purchaseorderlinetax_fact') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseorderlinetax_f/purchaseorderlinetax_f.py
-- Root method: PurchaseorderlinetaxFact.purchaseorderlinetax_factdetail [PurchaseOrderLineTax_FactDetail]
-- Inlined methods: PurchaseorderlinetaxFact.purchaseorderlinetax_factstage [PurchaseOrderLineTax_FactStage], PurchaseorderlinetaxFact.purchaseorderlinetax_factdetailmain [PurchaseOrderLineTax_FactDetailMain]
-- external_table_name: PurchaseOrderLineTax_FactDetail
-- schema_name: temp

WITH
purchaseorderlinetax_factstage AS (
    SELECT tt.taxamount * -1          AS TaxAmount_TransCur
             , tt.dataareaid              AS LegalEntityID
             , tt.currencycode            AS TaxCurrencyID
             , tt.taxcode                 AS TaxCodeID
             , CAST(tt.transdate AS DATE) AS TransDate
             , tt.sourcerecid             AS RecID_PL
             , tt.taxgroup                AS TaxGroupID
             , tt.modifieddatetime        AS _SourceDate
             , tt.recid                   AS _RecID

          FROM {{ ref('taxtrans') }}           tt
         INNER JOIN {{ ref('purchline') }}     pl
            ON pl.dataareaid  = tt.dataareaid
           AND pl.recid       = tt.sourcerecid
         INNER JOIN {{ ref('sqldictionary') }} sd
            ON sd.tabid       = tt.sourcetableid
           AND sd.name        = 'PurchLine'
         WHERE tt.taxamount <> 0;
),
purchaseorderlinetax_factdetailmain AS (
    SELECT le.LegalEntityKey                                                                 AS LegalEntityKey
             , dd.DateKey                                                                        AS TransDateKey
             , dpol.PurchaseOrderLineKey                                                         AS PurchaseOrderLineKey
             , dtc.TaxCodeKey                                                                    AS TaxCodeKey
             , dtg.TaxGroupKey                                                                   AS TaxGroupKey
             , cur.CurrencyKey                                                                   AS TaxCurrencyKey
             , ts.TaxAmount_TransCur                                                             AS TaxAmount_TransCur
             , ts.TaxAmount_TransCur * ISNULL(ex.ExchangeRate, 1)                                AS TaxAmount
             , ts.TaxCodeID                                                                      AS TaxCodeID
             , ts.LegalEntityID                                                                  AS LegalEntityID
             , dtc.TaxStartDate                                                                  AS TaxStartDate
             , CASE WHEN dtc.TaxEndDate = '1900-01-01' THEN '9999-12-31' ELSE dtc.TaxEndDate END AS TaxEndDate
             , ts.TransDate                                                                      AS TransDate
             , ts._SourceDate                                                                    AS _SourceDate
             , ts._RecID                                                                         AS _RecID
             , 1                                                                                 AS _SourceID

          FROM purchaseorderlinetax_factstage                     ts
         INNER JOIN silver.cma_LegalEntity       le
            ON le.LegalEntityID    = ts.LegalEntityID
         INNER JOIN silver.cma_Date              dd
            ON dd.Date             = ts.TransDate
          LEFT JOIN silver.cma_Currency          cur
            ON cur.CurrencyID      = ts.TaxCurrencyID
          LEFT JOIN silver.cma_PurchaseOrderLine dpol
            ON dpol._RecID         = ts.RecID_PL
           AND dpol._SourceID      = 1
          LEFT JOIN silver.cma_TaxGroup          dtg
            ON dtg.LegalEntityID   = ts.LegalEntityID
           AND dtg.TaxGroupID      = ts.TaxGroupID
          LEFT JOIN silver.cma_TaxCode           dtc
            ON dtc.LegalEntityID   = ts.LegalEntityID
           AND dtc.TaxCode         = ts.TaxCodeID
          LEFT JOIN silver.cma_ExchangeRate_Fact ex
            ON ex.ExchangeDateKey  = dd.DateKey
           AND ex.FromCurrencyID   = ts.TaxCurrencyID
           AND ex.ToCurrencyID     = le.AccountingCurrencyID
           AND ex.ExchangeRateType = le.TransExchangeRateType;
)
SELECT ROW_NUMBER() OVER (ORDER BY t1._RecID) AS PurchaseOrderLineTaxKey
        ,  t1.LegalEntityKey
         , t1.TransDateKey
         , t1.PurchaseOrderLineKey
         , t1.TaxGroupKey
         , t1.TaxCodeKey
         , t1.TaxCurrencyKey
         , t1.TaxAmount_TransCur
         , t1.TaxAmount
         , t1._SourceDate
         , t1._RecID
         , t1._SourceID
         , CURRENT_TIMESTAMP                                                            AS _CreatedDate
         , CURRENT_TIMESTAMP                                                            AS _ModifiedDate  

      FROM purchaseorderlinetax_factdetailmain t1
     WHERE t1.TransDate BETWEEN t1.TaxStartDate AND t1.TaxEndDate;
