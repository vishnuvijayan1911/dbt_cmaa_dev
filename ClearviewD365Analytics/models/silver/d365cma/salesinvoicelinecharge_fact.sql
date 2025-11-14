{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/salesinvoicelinecharge_fact/salesinvoicelinecharge_fact.py
-- Root method: SalesinvoicelinechargeFact.salesinvoicelinecharge_factdetail [SalesInvoiceLineCharge_FactDetail]
-- Inlined methods: SalesinvoicelinechargeFact.salesinvoicelinecharge_factstage [SalesInvoiceLineCharge_FactStage], SalesinvoicelinechargeFact.salesinvoicelinecharge_factcharge [SalesInvoiceLineCharge_FactCharge], SalesinvoicelinechargeFact.salesinvoicelinecharge_factdetail1 [SalesInvoiceLineCharge_FactDetail1]
-- external_table_name: SalesInvoiceLineCharge_FactDetail
-- schema_name: temp

WITH
salesinvoicelinecharge_factstage AS (
    SELECT t.ChargeCurrencyID
             , t.LegalEntityID
             , t.MarkupCategoryID
             , t.Code
             , t.ModuleType
             , t.ChargeTypeID
             , t.TransCurrencyID
             , t.VoucherID
             , t.ExchangeRate
             , t.IncludedCharge
             , t.AdditionalCharge + t.BillableHeaderCharge     AS AdditionalCharge
             , t.NonBillableCharge + t.NonBillableHeaderCharge AS NonBillableCharge
             , t.TaxAmount
             , t.TotalCharge
             , t.PriceUnit
             , t.IncludeInTotalPrice
             , t.PrintCharges
             , t.TransDate
             , t._RECID1
             , t._RECID

          FROM (   SELECT mt.currencycode                                                                               AS ChargeCurrencyID
                        , mt.dataareaid                                                                                AS LegalEntityID
                        , mt.markupcategory                                                                             AS MarkupCategoryID
                        , mt.markupcode                                                                                 AS Code
                        , mt.moduletype                                                                                 AS ModuleType
                        , mu.custtype                                                                                   AS ChargeTypeID
                        , cit.currencycode                                                                              AS TransCurrencyID
                        , mt.voucher                                                                                    AS VoucherID
                        , cij.exchrate                                                                                  AS ExchangeRate
                        , 0                                                                                             AS IncludedCharge
                        , 0                                                                                             AS AdditionalCharge
                        , 0                                                                                             AS NonBillableCharge
                        , CASE WHEN mu.custtype <> 1
                               THEN mt.calculatedamount / (COUNT (cit.recid) OVER (PARTITION BY mt.recid)) ELSE 0 END AS BillableHeaderCharge
                        , CASE WHEN mu.custtype = 1
                               THEN mt.calculatedamount / (COUNT (cit.recid) OVER (PARTITION BY mt.recid)) ELSE 0 END AS NonBillableHeaderCharge
                        , mt.taxamount                                                                                  AS TaxAmount
                        , 0                                                                                             AS TotalCharge
                        , mt.cmapriceuom                                                                        AS PriceUnit
                        , 0                                                                                             AS IncludeInTotalPrice
                        , 0                                                                                             AS PrintCharges
                        , CAST(mt.transdate AS DATE)                                                                    AS TransDate
                        , cit.recid                                                                                   AS _RECID1
                        , mt.recid                                                                                     AS _RECID
                     FROM {{ ref('markuptrans') }}           mt
                    INNER JOIN {{ ref('markuptable') }}      mu
                       ON mu.dataareaid          = mt.dataareaid
                      AND mu.markupcode           = mt.markupcode
                      AND mu.moduletype           = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }}    sd
                       ON sd.fieldid              = 0
                      AND sd.tabid                = mt.transtableid
                      AND sd.name                 = 'CustInvoiceJour'
                    INNER JOIN {{ ref('custinvoicejour') }}  cij
                       ON cij.recid              = mt.transrecid 
                    INNER JOIN {{ ref('custinvoicetrans') }} cit
                       ON cit.dataareaid         = cij.dataareaid
                      AND cit.salesid             = cij.salesid
                      AND cit.invoiceid           = cij.invoiceid
                      AND cit.invoicedate         = cij.invoicedate
                      AND cit.numbersequencegroup = cij.numbersequencegroup
                   UNION
                   SELECT mt.currencycode                                                                     AS ChargeCurrencyID
                        , mt.dataareaid                                                                      AS LegalEntityID
                        , mt.markupcategory                                                                   AS MarkupCategoryID
                        , mt.markupcode                                                                       AS Code
                        , mt.moduletype                                                                       AS ModuleType
                        , mu.custtype                                                                         AS ChargeTypeID
                        , mt.currencycode                                                                     AS TransCurrencyID
                        , mt.voucher                                                                          AS VoucherID
                        , cij.exchrate                                                                        AS ExchangeRate
                        , CASE WHEN mt.cmarollup = 1 AND mu.custtype <> 1 THEN mt.calculatedamount ELSE 0 END AS IncludedCharge
                        , CASE WHEN mt.cmarollup = 0 AND mu.custtype <> 1 THEN mt.calculatedamount ELSE 0 END AS AdditionalCharge
                        , CASE WHEN mu.custtype = 1 THEN mt.calculatedamount ELSE 0 END                       AS NonBillableCharge
                        , 0                                                                                   AS BillableHeaderCharge
                        , 0                                                                                   AS NonBillableHeaderCharge
                        , mt.taxamount                                                                        AS TaxAmount
                        , mt.calculatedamount                                                                 AS TotalCharge
                        , mt.cmapriceuom                                                              AS PriceUnit
                        , mt.cmarollup                                                                        AS IncludeInTotalPrice
                        , mt.cmatoprint                                                                       AS PrintCharges
                        , CAST(mt.transdate AS DATE)                                                          AS TransDate
                        , cit.recid                                                                          AS _RECID1
                        , mt.recid                                                                           AS _RECID
                     FROM {{ ref('markuptrans') }}           mt
                    INNER JOIN {{ ref('markuptable') }}      mu
                       ON mu.dataareaid          = mt.dataareaid
                      AND mu.markupcode           = mt.markupcode
                      AND mu.moduletype           = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }}    sd
                       ON sd.fieldid              = 0
                      AND sd.tabid                = mt.transtableid
                      AND sd.name                 = 'CUSTINVOICETRANS'
                    INNER JOIN {{ ref('custinvoicetrans') }} cit
                       ON cit.recid              = mt.transrecid
                    INNER JOIN {{ ref('custinvoicejour') }}  cij
                       ON cij.dataareaid         = cit.dataareaid
                      AND cij.salesid             = cit.salesid
                      AND cij.invoiceid           = cit.invoiceid
                      AND cij.invoicedate         = cit.invoicedate
                      AND cij.numbersequencegroup = cit.numbersequencegroup) AS t;
),
salesinvoicelinecharge_factcharge AS (
    SELECT ts.ChargeCurrencyID                                AS ChargeCurrencyID
             , ts.LegalEntityID                                   AS LegalEntityID
             , ts.MarkupCategoryID                                AS MarkupCategoryID
             , ts.Code                                            AS Code
             , ts.ModuleType                                      AS ModuleType
             , ts.ChargeTypeID                                    AS ChargeTypeID
             , ts.TransCurrencyID                                 AS TransCurrencyID
             , ts.VoucherID                                       AS VoucherID
             , ts.ExchangeRate                                    AS ExchangeRate
             , ts.AdditionalCharge * ISNULL (ex.ExchangeRate, 1)  AS AdditionalCharge_TransCur
             , ts.IncludedCharge * ISNULL (ex.ExchangeRate, 1)    AS IncludedCharge_TransCur
             , ts.NonBillableCharge * ISNULL (ex.ExchangeRate, 1) AS NonBillableCharge_TransCur
             , ts.TaxAmount * ISNULL (ex.ExchangeRate, 1)         AS TaxAmount_TransCur
             , ts.TotalCharge * ISNULL (ex.ExchangeRate, 1)       AS TotalCharges_TransCur
             , ts.PriceUnit
             , ts.IncludeInTotalPrice
             , ts.PrintCharges
             , ts.TransDate
             , ts._RECID1
             , ts._RECID

          FROM salesinvoicelinecharge_factstage                     ts
         INNER JOIN silver.cma_LegalEntity       le
            ON le.LegalEntityID    = ts.LegalEntityID
          LEFT JOIN silver.cma_Date              dd
            ON dd.Date             = ts.TransDate
          LEFT JOIN silver.cma_ExchangeRate_Fact ex
            ON ex.ExchangeDateKey  = dd.DateKey
           AND ex.FromCurrencyID   = ts.ChargeCurrencyID
           AND ex.ToCurrencyID     = ts.TransCurrencyID
           AND ex.ExchangeRateType = le.TransExchangeRateType;
),
salesinvoicelinecharge_factdetail1 AS (
    SELECT dc.ChargeCodeKey                                                    AS ChargeCodeKey
             , dcc.ChargeCategoryKey                                               AS ChargeCategoryKey
             , cur.CurrencyKey                                                     AS ChargeCurrencyKey
             , ct.ChargeTypeKey                                                    AS ChargeTypeKey
             , le.LegalEntityKey                                                   AS LegalEntityKey
             , du.UOMKey                                                           AS PricingUOMKey
             , dsil.SalesInvoiceLineKey                                            AS SalesInvoiceLineKey
             , dd.DateKey                                                          AS TransDateKey
             , dv.VoucherKey                                                       AS VoucherKey
             , ts.ExchangeRate                                                     AS ExchangeRate
             , CASE WHEN ts.ExchangeRate = 0
                    THEN ts.AdditionalCharge_TransCur * ISNULL (ex1.ExchangeRate, 1)
                    ELSE ts.AdditionalCharge_TransCur * ts.ExchangeRate / 100 END  AS AdditionalCharge
             , ts.AdditionalCharge_TransCur                                        AS AdditionalCharge_TransCur
             , CASE WHEN ts.ExchangeRate = 0
                    THEN ts.IncludedCharge_TransCur * ISNULL (ex1.ExchangeRate, 1)
                    ELSE ts.IncludedCharge_TransCur * ts.ExchangeRate / 100 END    AS IncludedCharge
             , ts.IncludedCharge_TransCur                                          AS IncludedCharge_TransCur
             , ts.IncludeInTotalPrice                                              AS IncludeInTotalPrice
             , CASE WHEN ts.ExchangeRate = 0
                    THEN ts.NonBillableCharge_TransCur * ISNULL (ex1.ExchangeRate, 1)
                    ELSE ts.NonBillableCharge_TransCur * ts.ExchangeRate / 100 END AS NonBillableCharge
             , ts.NonBillableCharge_TransCur                                       AS NonBillableCharge_TransCur
             , ts.PrintCharges                                                     AS PrintCharges
             , CASE WHEN ts.ExchangeRate = 0
                    THEN ts.TaxAmount_TransCur * ISNULL (ex1.ExchangeRate, 1)
                    ELSE ts.TaxAmount_TransCur * ts.ExchangeRate / 100 END         AS TaxAmount
             , ts.TaxAmount_TransCur                                               AS TaxAmount_TransCur
             , CASE WHEN ts.ExchangeRate = 0
                    THEN ts.TotalCharges_TransCur * ISNULL (ex1.ExchangeRate, 1)
                    ELSE ts.TotalCharges_TransCur * ts.ExchangeRate / 100 END      AS TotalCharges
             , ts.TotalCharges_TransCur                                            AS TotalCharges_TransCur
             , ts._RECID                                                           AS _RecID1
             , ts._RECID1                                                          AS _RecID2
             , 1                                                                   AS _SourceID

          FROM salesinvoicelinecharge_factcharge                    ts
         INNER JOIN silver.cma_LegalEntity       le
            ON le.LegalEntityID     = ts.LegalEntityID
          LEFT JOIN silver.cma_ChargeCode        dc
            ON dc.LegalEntityID     = ts.LegalEntityID
           AND dc.ModuleTypeID      = ts.ModuleType
           AND dc.ChargeCode        = ts.Code
          LEFT JOIN silver.cma_ChargeCategory    dcc
            ON dcc.ChargeCategoryID = ts.MarkupCategoryID
          LEFT JOIN silver.cma_Date              dd
            ON dd.Date              = ts.TransDate
          LEFT JOIN silver.cma_Currency          cur
            ON cur.CurrencyID       = ts.ChargeCurrencyID
          LEFT JOIN silver.cma_SalesInvoiceLine  dsil
            ON dsil._RecID2          = ts._RECID1
           AND dsil._SourceID       = 1
          LEFT JOIN silver.cma_Voucher           dv
            ON dv.LegalEntityID     = ts.LegalEntityID
           AND dv.VoucherID         = ts.VoucherID
          LEFT JOIN silver.cma_ChargeType        ct
            ON ct.ChargeTypeID      = ts.ChargeTypeID
          LEFT JOIN silver.cma_UOM               du
            ON du.UOM               = ts.PriceUnit
          LEFT JOIN silver.cma_ExchangeRate_Fact ex1
            ON ex1.ExchangeDateKey  = dd.DateKey
           AND ex1.FromCurrencyID   = ts.TransCurrencyID
           AND ex1.ToCurrencyID     = le.AccountingCurrencyID
           AND ex1.ExchangeRateType = le.TransExchangeRateType;
)
SELECT ROW_NUMBER() OVER (ORDER BY td._RecID1) AS SalesInvoiceLineChargeKey
        , td.ChargeCodeKey
         , td.ChargeCategoryKey
         , td.ChargeCurrencyKey
         , td.ChargeTypeKey
         , td.LegalEntityKey
         , td.PricingUOMKey
         , td.SalesInvoiceLineKey
         , td.TransDateKey
         , td.VoucherKey
         , td.AdditionalCharge
         , td.AdditionalCharge * ISNULL (ex.ExchangeRate, 1)   AS AdditionalCharge_CAD
         , td.AdditionalCharge * ISNULL (ex1.ExchangeRate, 1)  AS AdditionalCharge_MXP
         , td.AdditionalCharge * ISNULL (ex2.ExchangeRate, 1)  AS AdditionalCharge_USD
         , td.AdditionalCharge_TransCur
         , td.IncludedCharge
         , td.IncludedCharge * ISNULL (ex.ExchangeRate, 1)     AS IncludedCharge_CAD
         , td.IncludedCharge * ISNULL (ex1.ExchangeRate, 1)    AS IncludedCharge_MXP
         , td.IncludedCharge * ISNULL (ex2.ExchangeRate, 1)    AS IncludedCharge_USD
         , td.IncludedCharge_TransCur
         , td.IncludeInTotalPrice
         , td.NonBillableCharge
         , td.NonBillableCharge * ISNULL (ex.ExchangeRate, 1)  AS NonBillableCharge_CAD
         , td.NonBillableCharge * ISNULL (ex1.ExchangeRate, 1) AS NonBillableCharge_MXP
         , td.NonBillableCharge * ISNULL (ex2.ExchangeRate, 1) AS NonBillableCharge_USD
         , td.NonBillableCharge_TransCur
         , td.PrintCharges
         , td.TaxAmount
         , td.TaxAmount * ISNULL (ex.ExchangeRate, 1)          AS TaxAmount_CAD
         , td.TaxAmount * ISNULL (ex1.ExchangeRate, 1)         AS TaxAmount_MXP
         , td.TaxAmount * ISNULL (ex2.ExchangeRate, 1)         AS TaxAmount_USD
         , td.TaxAmount_TransCur
         , td.TotalCharges
         , td.TotalCharges * ISNULL (ex.ExchangeRate, 1)       AS TotalCharges_CAD
         , td.TotalCharges * ISNULL (ex1.ExchangeRate, 1)      AS TotalCharges_MXP
         , td.TotalCharges * ISNULL (ex2.ExchangeRate, 1)      AS TotalCharges_USD
         , td.TotalCharges_TransCur
         , td._RecID1
         , td._RecID2
         , td._SourceID
         , CURRENT_TIMESTAMP                                                            AS _CreatedDate
         , CURRENT_TIMESTAMP                                                            AS _ModifiedDate  

      FROM salesinvoicelinecharge_factdetail1                   td
      LEFT JOIN silver.cma_LegalEntity       le
        ON le.LegalEntityKey    = td.LegalEntityKey
      LEFT JOIN silver.cma_ExchangeRate_Fact ex
        ON ex.ExchangeDateKey   = td.TransDateKey
       AND ex.FromCurrencyID    = le.AccountingCurrencyID
       AND ex.ToCurrencyID      = 'CAD'
       AND ex.ExchangeRateType  = le.TransExchangeRateType
      LEFT JOIN silver.cma_ExchangeRate_Fact ex1
        ON ex1.ExchangeDateKey   = td.TransDateKey
       AND ex1.FromCurrencyID   = le.AccountingCurrencyID
       AND ex1.ToCurrencyID     = 'MXN'
       AND ex1.ExchangeRateType = le.TransExchangeRateType
      LEFT JOIN silver.cma_ExchangeRate_Fact ex2
        ON ex2.ExchangeDateKey   = td.TransDateKey
       AND ex2.FromCurrencyID   = le.AccountingCurrencyID
       AND ex2.ToCurrencyID     = 'USD'
       AND ex2.ExchangeRateType = le.TransExchangeRateType;
