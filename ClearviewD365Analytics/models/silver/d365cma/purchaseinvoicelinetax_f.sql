{{ config(materialized='table', tags=['silver'], alias='purchaseinvoicelinetax_fact') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseinvoicelinetax_f/purchaseinvoicelinetax_f.py
-- Root method: PurchaseinvoicelinetaxFact.purchaseinvoicelinetax_factdetail [PurchaseInvoiceLineTax_FactDetail]
-- Inlined methods: PurchaseinvoicelinetaxFact.purchaseinvoicelinetax_factstage [PurchaseInvoiceLineTax_FactStage], PurchaseinvoicelinetaxFact.purchaseinvoicelinetax_factdetail1 [PurchaseInvoiceLineTax_FactDetail1]
-- external_table_name: PurchaseInvoiceLineTax_FactDetail
-- schema_name: temp

WITH
purchaseinvoicelinetax_factstage AS (
    SELECT tt.taxamount * -1          AS TaxAmount_TransCur
             , tt.dataareaid              AS LegalEntityID
             , tt.currencycode            AS TaxCurrencyID
             , tt.taxcode                 AS TaxCodeID
             , CAST(tt.transdate AS DATE) AS TransDate
             , tt.sourcerecid             AS RecID_VIT
             , tt.taxgroup                AS TaxGroupID
             , tt.modifieddatetime        AS _SourceDate
             , tt.recid                   AS _RecID

          FROM {{ ref('taxtrans') }}              tt
         INNER JOIN {{ ref('vendinvoicetrans') }} vit
            ON vit.dataareaid  = tt.dataareaid
           AND vit.recid       = tt.sourcerecid
         INNER JOIN {{ ref('sqldictionary') }}    sd
            ON sd.tabid        = tt.sourcetableid
           AND sd.name         = 'VendInvoiceTrans'
         WHERE tt.taxamount <> 0;
),
purchaseinvoicelinetax_factdetail1 AS (
    SELECT ROW_NUMBER() OVER (ORDER BY ts._RecID) AS PurchaseInvoiceLineTaxKey
             , le.LegalEntityKey                                                                 AS LegalEntityKey
             , dd.DateKey                                                                        AS TransDateKey
             , dpil.PurchaseInvoiceLineKey                                                       AS PurchaseInvoiceLineKey
             , dtg.TaxGroupKey                                                                   AS TaxGroupKey
             , dtc.TaxCodeKey                                                                    AS TaxCodeKey
             , cur.CurrencyKey                                                                   AS TaxCurrencyKey
             , ts.LegalEntityID                                                                  AS LegalEntityID
             , ts.TaxCodeID                                                                      AS TaxCodeID
             , ts.TaxAmount_TransCur                                                             AS TaxAmount_TransCur
             , ts.TaxAmount_TransCur * ISNULL(ex.ExchangeRate, 1)                                AS TaxAmount
             , dtc.TaxStartDate                                                                  AS TaxStartDate
             , CASE WHEN dtc.TaxEndDate = '1900-01-01' THEN '9999-12-31' ELSE dtc.TaxEndDate END AS TaxEndDate
             , ts.TransDate                                                                      AS TransDate
             , ts._SourceDate                                                                    AS _SourceDate
             , ts._RecID                                                                         AS _RecID
             , 1                                                                                 AS _SourceID

          FROM purchaseinvoicelinetax_factstage                       ts
         INNER JOIN silver.cma_LegalEntity         le
            ON le.LegalEntityID    = ts.LegalEntityID
         INNER JOIN silver.cma_Date                dd
            ON dd.Date             = ts.TransDate
          LEFT JOIN silver.cma_Currency            cur
            ON cur.CurrencyID      = ts.TaxCurrencyID
          LEFT JOIN silver.cma_PurchaseInvoiceLine dpil
            ON dpil._RecID2         = ts.RecID_VIT
           AND dpil._SourceID      = 1
          LEFT JOIN silver.cma_TaxGroup            dtg
            ON dtg.LegalEntityID   = ts.LegalEntityID
           AND dtg.TaxGroupID      = ts.TaxGroupID
          LEFT JOIN silver.cma_TaxCode             dtc
            ON dtc.LegalEntityID   = ts.LegalEntityID
           AND dtc.TaxCode         = ts.TaxCodeID
          LEFT JOIN silver.cma_ExchangeRate_Fact   ex
            ON ex.ExchangeDateKey  = dd.DateKey
           AND ex.FromCurrencyID   = ts.TaxCurrencyID
           AND ex.ToCurrencyID     = le.AccountingCurrencyID
           AND ex.ExchangeRateType = le.TransExchangeRateType;
)
SELECT td.PurchaseInvoiceLineTaxKey
         , td.LegalEntityKey
         , td.TransDateKey
         , td.PurchaseInvoiceLineKey
         , td.TaxGroupKey
         , td.TaxCodeKey
         , td.TaxCurrencyKey
         , td.TaxAmount_TransCur
         , td.TaxAmount
         , td._SourceDate
         , td._RecID
         , td._SourceID
         , CURRENT_TIMESTAMP                                                            AS _CreatedDate
         , CURRENT_TIMESTAMP                                                            AS _ModifiedDate  

      FROM purchaseinvoicelinetax_factdetail1 td
     WHERE td.TransDate BETWEEN td.TaxStartDate AND td.TaxEndDate;
