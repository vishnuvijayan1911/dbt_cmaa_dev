{{ config(materialized='table', tags=['silver'], alias='salesorderlinetax_fact') }}

-- Source file: cma/cma/layers/_base/_silver/salesorderlinetax_f/salesorderlinetax_f.py
-- Root method: SalesorderlinetaxFact.salesorderlinetax_factdetail [SalesOrderLineTax_FactDetail]
-- Inlined methods: SalesorderlinetaxFact.salesorderlinetax_factstage [SalesOrderLineTax_FactStage], SalesorderlinetaxFact.salesorderlinetax_factdetailmain [SalesOrderLineTax_FactDetailMain]
-- external_table_name: SalesOrderLineTax_FactDetail
-- schema_name: temp

WITH
salesorderlinetax_factstage AS (
    SELECT tt.taxamount * -1          AS TaxAmount_TransCur
             , tt.dataareaid              AS LegalEntityID
             , tt.currencycode            AS TaxCurrencyID
             , tt.taxcode                 AS TaxCodeID
             , CAST(tt.transdate AS DATE) AS TransDate
             , tt.sourcerecid             AS RecID_SL
             , tt.taxgroup                AS TaxGroupID
             , tt.modifieddatetime       AS _SourceDate
             , tt.recid                   AS _RecID

          FROM {{ ref('taxtrans') }}           tt
         INNER JOIN {{ ref('salesline') }}     sl
            ON sl.recid  = tt.sourcerecid
         INNER JOIN {{ ref('sqldictionary') }} sd
            ON sd.tabid  = tt.sourcetableid
           AND sd.name   = 'SalesLine'
         WHERE tt.taxamount <> 0;
),
salesorderlinetax_factdetailmain AS (
    SELECT le.LegalEntityKey                                                                 AS LegalEntityKey
             , dd.DateKey                                                                        AS TransDateKey
             , dsol.SalesOrderLineKey                                                            AS SalesOrderLineKey
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

          FROM salesorderlinetax_factstage                     ts
         INNER JOIN {{ ref('legalentity_d') }}       le
            ON le.LegalEntityID    = ts.LegalEntityID
         INNER JOIN {{ ref('date_d') }}              dd
            ON dd.Date             = ts.TransDate
          LEFT JOIN {{ ref('currency_d') }}          cur
            ON cur.CurrencyID      = ts.TaxCurrencyID
          LEFT JOIN {{ ref('salesorderline_d') }}    dsol
            ON dsol._RecID         = ts.RecID_SL
           AND dsol._SourceID      = 1
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
SELECT ROW_NUMBER() OVER (ORDER BY t1._RecID) AS SalesOrderLineTaxKey
         , t1.LegalEntityKey
         , t1.TransDateKey
         , t1.SalesOrderLineKey
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

      FROM salesorderlinetax_factdetailmain t1
     WHERE t1.TransDate BETWEEN t1.TaxStartDate AND t1.TaxEndDate;
