{{ config(materialized='table', tags=['silver'], alias='salesinvoicelinetax_fact') }}

-- Source file: cma/cma/layers/_base/_silver/salesinvoicelinetax_f/salesinvoicelinetax_f.py
-- Root method: SalesinvoicelinetaxFact.salesinvoicelinetax_factdetail [SalesInvoiceLineTax_FactDetail]
-- Inlined methods: SalesinvoicelinetaxFact.salesinvoicelinetax_factstage [SalesInvoiceLineTax_FactStage], SalesinvoicelinetaxFact.salesinvoicelinetax_factdetail1 [SalesInvoiceLineTax_FactDetail1]
-- external_table_name: SalesInvoiceLineTax_FactDetail
-- schema_name: temp

WITH
salesinvoicelinetax_factstage AS (
    SELECT tt.taxamount * -1          AS TaxAmount_TransCur
             , tt.dataareaid              AS LegalEntityID
             , tt.currencycode            AS TaxCurrencyID
             , tt.taxcode                 AS TaxCodeID
             , CAST(tt.transdate AS DATE) AS TransDate
             , tt.sourcerecid             AS RecID_CIT
             , tt.taxgroup                AS TaxGroupID
             , tt.modifieddatetime       AS _SourceDate
             , tt.recid                   AS _RecID

          FROM {{ ref('taxtrans') }}              tt
         INNER JOIN {{ ref('custinvoicetrans') }} cit
            ON cit.recid  = tt.sourcerecid
         INNER JOIN {{ ref('sqldictionary') }}    sd
            ON sd.tabid   = tt.sourcetableid
           AND sd.name    = 'CustInvoiceTrans'
         WHERE tt.taxamount <> 0;
),
salesinvoicelinetax_factdetail1 AS (
    SELECT le.LegalEntityKey                                                                 AS LegalEntityKey
             , dd.DateKey                                                                        AS TransDateKey
             , dsil.SalesInvoiceLineKey                                                          AS SalesInvoiceLineKey
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

          FROM salesinvoicelinetax_factstage                     ts
         INNER JOIN {{ ref('legalentity_d') }}       le
            ON le.LegalEntityID    = ts.LegalEntityID
         INNER JOIN {{ ref('date_d') }}              dd
            ON dd.Date             = ts.TransDate
          LEFT JOIN {{ ref('currency_d') }}          cur
            ON cur.CurrencyID      = ts.TaxCurrencyID
          LEFT JOIN {{ ref('salesinvoiceline_d') }}  dsil
            ON dsil._RecID1          = ts.RecID_CIT
           AND dsil._SourceID      = 1
          LEFT JOIN {{ ref('taxgroup_d') }}          dtg
            ON dtg.LegalEntityID   = ts.LegalEntityID
           AND dtg.TaxGroupID      = ts.TaxGroupID
          LEFT JOIN {{ ref('taxcode_d') }}           dtc
            ON dtc.LegalEntityID   = ts.LegalEntityID
           AND dtc.TaxCode         = ts.TaxCodeID
          LEFT JOIN {{ ref('exchangerate_f') }} ex
            ON ex.ExchangeDateKey  = dd.DateKey
           AND ex.FromCurrencyID   = ts.TaxCurrencyID
           AND ex.ToCurrencyID     = le.AccountingCurrencyID
           AND ex.ExchangeRateType = le.TransExchangeRateType;
)
SELECT ROW_NUMBER() OVER (ORDER BY td._RecID) AS SalesInvoiceLineTaxKey
         , td.LegalEntityKey
         , td.TransDateKey
         , td.SalesInvoiceLineKey
         , td.TaxGroupKey
         , td.TaxCodeKey
         , td.TaxCurrencyKey
         , td.TaxAmount_TransCur
         , td.TaxAmount
         , td._SourceDate
         , td._RecID
         , td._SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                            AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                            AS _ModifiedDate  
      FROM salesinvoicelinetax_factdetail1 td
     WHERE td.TransDate BETWEEN td.TaxStartDate AND td.TaxEndDate;
