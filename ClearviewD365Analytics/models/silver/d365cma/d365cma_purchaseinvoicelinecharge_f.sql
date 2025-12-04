{{ config(materialized='table', tags=['silver'], alias='purchaseinvoicelinecharge_fact') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseinvoicelinecharge_f/purchaseinvoicelinecharge_f.py
-- Root method: PurchaseinvoicelinechargeFact.purchaseinvoicelinecharge_factdetail [PurchaseInvoiceLineCharge_FactDetail]
-- Inlined methods: PurchaseinvoicelinechargeFact.purchaseinvoicelinecharge_factstage [PurchaseInvoiceLineCharge_FactStage], PurchaseinvoicelinechargeFact.purchaseinvoicelinecharge_factcharge [PurchaseInvoiceLineCharge_FactCharge]
-- external_table_name: PurchaseInvoiceLineCharge_FactDetail
-- schema_name: temp

WITH
purchaseinvoicelinecharge_factstage AS (
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
             , t._RecID1
             , t._RecID

          FROM (   SELECT mt.currencycode                                                                               AS ChargeCurrencyID
                        , mt.dataareaid                                                                                AS LegalEntityID
                        , mt.markupcategory                                                                             AS MarkupCategoryID
                        , mt.markupcode                                                                                 AS Code
                        , mt.moduletype                                                                                 AS ModuleType
                        , mu.custtype                                                                                   AS ChargeTypeID
                        , vit.currencycode                                                                              AS TransCurrencyID
                        , vij.exchrate                                                                                  AS ExchangeRate
                        , mt.voucher                                                                                    AS VoucherID
                        , 0                                                                                             AS IncludedCharge
                        , 0                                                                                             AS AdditionalCharge
                        , 0                                                                                             AS NonBillableCharge
                        , CASE WHEN mu.custtype <> 1
                               THEN mt.calculatedamount / (COUNT (vit.recid) OVER (PARTITION BY mt.recid)) ELSE 0 END AS BillableHeaderCharge
                        , CASE WHEN mu.custtype = 1
                               THEN mt.calculatedamount / (COUNT (vit.recid) OVER (PARTITION BY mt.recid)) ELSE 0 END AS NonBillableHeaderCharge
                        , mt.taxamount                                                                                  AS TaxAmount
                        , 0                                                                                             AS TotalCharge
                        , mt.cmapriceuom                                                                        AS PriceUnit
                        , 0                                                                                             AS IncludeInTotalPrice
                        , 0                                                                                             AS PrintCharges
                        , CAST(mt.transdate AS DATE)                                                                    AS TransDate
                        , vit.recid                                                                                    AS _RecID1
                        , mt.recid                                                                                     AS _RecID
                     FROM {{ ref('markuptrans') }}           mt
                    INNER JOIN {{ ref('markuptable') }}      mu
                       ON mu.dataareaid           = mt.dataareaid
                      AND mu.markupcode           = mt.markupcode
                      AND mu.moduletype           = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }}    sd
                       ON sd.fieldid              = 0
                      AND sd.tabid                = mt.transtableid
                      AND sd.name                 = 'VendInvoiceJour'
                    INNER JOIN {{ ref('vendinvoicejour') }}  vij
                       ON vij.recid              = mt.transrecid
                    INNER JOIN {{ ref('vendinvoicetrans') }} vit
                       ON vit.dataareaid          = vij.dataareaid
                      AND vit.purchid             = vij.purchid
                      AND vit.invoiceid           = vij.invoiceid
                      AND vit.invoicedate         = vij.invoicedate
                      AND vit.numbersequencegroup = vij.numbersequencegroup
                      AND vit.internalinvoiceid   = vij.internalinvoiceid
                   UNION
                   SELECT mt.currencycode                                                                     AS ChargeCurrencyID
                        , mt.dataareaid                                                                      AS LegalEntityID
                        , mt.markupcategory                                                                   AS MarkupCategoryID
                        , mt.markupcode                                                                       AS Code
                        , mt.moduletype                                                                       AS ModuleType
                        , mu.custtype                                                                         AS ChargeTypeID
                        , vit.currencycode                                                                    AS TransCurrencyID
                        , vij.exchrate                                                                        AS ExchangeRate
                        , mt.voucher                                                                          AS VoucherID
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
                        , vit.recid                                                                          AS _RecID1
                        , mt.recid                                                                           AS _RecID
                     FROM {{ ref('markuptrans') }}           mt
                    INNER JOIN {{ ref('markuptable') }}      mu
                       ON mu.dataareaid           = mt.dataareaid
                      AND mu.markupcode           = mt.markupcode
                      AND mu.moduletype           = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }}    sd
                       ON sd.fieldid              = 0
                      AND sd.tabid                = mt.transtableid
                      AND sd.name                 = 'VENDINVOICETRANS'
                    INNER JOIN {{ ref('vendinvoicetrans') }} vit
                       ON vit.recid               = mt.transrecid
                    INNER JOIN {{ ref('vendinvoicejour') }}  vij
                       ON vij.dataareaid          = vit.dataareaid
                      AND vij.purchid             = vit.purchid
                      AND vij.invoiceid           = vit.invoiceid
                      AND vij.invoicedate         = vit.invoicedate
                      AND vij.numbersequencegroup = vit.numbersequencegroup
                      AND vij.internalinvoiceid   = vit.internalinvoiceid) AS t;
),
purchaseinvoicelinecharge_factcharge AS (
    SELECT ts.ChargeCurrencyID                                AS ChargeCurrencyID
             , ts.LegalEntityID                                   AS LegalEntityID
             , ts.MarkupCategoryID                                AS MarkupCategoryID
             , ts.Code                                            AS Code
             , ts.ModuleType                                      AS ModuleType
             , ts.ChargeTypeID                                    AS ChargeTypeID
             , ts.TransCurrencyID                                 AS TransCurrencyID
             , ts.VoucherID                                       AS VoucherID
             , ts.ExchangeRate                                    AS ExchangeRate
             , ts.AdditionalCharge                                AS AdditionalCharge
             , ts.AdditionalCharge * ISNULL (ex.ExchangeRate, 1)  AS AdditionalCharge_TransCur
             , ts.IncludedCharge * ISNULL (ex.ExchangeRate, 1)    AS IncludedCharge_TransCur
             , ts.NonBillableCharge * ISNULL (ex.ExchangeRate, 1) AS NonBillableCharge_TransCur
             , ts.TaxAmount * ISNULL (ex.ExchangeRate, 1)         AS TaxAmount_TransCur
             , ts.TotalCharge * ISNULL (ex.ExchangeRate, 1)       AS TotalCharges_TransCur
             , ts.PriceUnit                                       AS PriceUnit
             , ts.IncludeInTotalPrice                             AS IncludeInTotalPrice
             , ts.PrintCharges                                    AS PrintCharges
             , ts.TransDate                                       AS TransDate
             , ts._RecID1                                         AS _RecID1
             , ts._RecID                                          AS _RecID

          FROM purchaseinvoicelinecharge_factstage                     ts
         INNER JOIN {{ ref('d365cma_legalentity_d') }}       le
            ON le.LegalEntityID    = ts.LegalEntityID
          LEFT JOIN {{ ref('d365cma_date_d') }}              dd
            ON dd.Date             = ts.TransDate
          LEFT JOIN {{ ref('d365cma_exchangerate_f') }} ex
            ON ex.ExchangeDateKey  = dd.DateKey
           AND ex.FromCurrencyID   = ts.ChargeCurrencyID
           AND ex.ToCurrencyID     = ts.TransCurrencyID
           AND ex.ExchangeRateType = le.TransExchangeRateType;
)
SELECT {{ dbt_utils.generate_surrogate_key(['ts._RecID', 'ts._RecID1']) }} AS PurchaseInvoiceLineChargeKey
      ,dc.ChargeCodeKey                                                    AS ChargeCodeKey
         , dcc.ChargeCategoryKey                                               AS ChargeCategoryKey
         , cur.CurrencyKey                                                     AS ChargeCurrencyKey
         , ct.ChargeTypeKey                                                    AS ChargeTypeKey
         , le.LegalEntityKey                                                   AS LegalEntityKey
         , du.UOMKey                                                           AS PricingUOMKey
         , dvil.PurchaseInvoiceLineKey                                         AS PurchaseInvoiceLineKey
         , dd.DateKey                                                          AS TransDateKey
         , dv.VoucherKey                                                       AS VoucherKey
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
         , ts._RecID                                                           AS _RecID1
         , ts._RecID1                                                           AS _RecID2
         , 1                                                                   AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                            AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                            AS _ModifiedDate  
      FROM purchaseinvoicelinecharge_factcharge                      ts
     INNER JOIN {{ ref('d365cma_legalentity_d') }}         le
        ON le.LegalEntityID     = ts.LegalEntityID
      LEFT JOIN {{ ref('d365cma_chargecode_d') }}          dc
        ON dc.LegalEntityID     = ts.LegalEntityID
       AND dc.ModuleTypeID      = ts.ModuleType
       AND dc.ChargeCode        = ts.Code
      LEFT JOIN {{ ref('d365cma_chargecategory_d') }}      dcc
        ON dcc.ChargeCategoryID = ts.MarkupCategoryID
      LEFT JOIN {{ ref('d365cma_date_d') }}                dd
        ON dd.Date              = ts.TransDate
      LEFT JOIN {{ ref('d365cma_currency_d') }}            cur
        ON cur.CurrencyID       = ts.ChargeCurrencyID
      LEFT JOIN {{ ref('d365cma_purchaseinvoiceline_d') }} dvil
        ON dvil._RecID2          = ts._RecID1
       AND dvil._SourceID       = 1
      LEFT JOIN {{ ref('d365cma_voucher_d') }}             dv
        ON dv.LegalEntityID     = ts.LegalEntityID
       AND dv.VoucherID         = ts.VoucherID
      LEFT JOIN {{ ref('d365cma_chargetype_d') }}          ct
        ON ct.ChargeTypeID      = ts.ChargeTypeID
      LEFT JOIN {{ ref('d365cma_uom_d') }}                 du
        ON du.UOM               = ts.PriceUnit
      LEFT JOIN {{ ref('d365cma_exchangerate_f') }}   ex1
        ON ex1.ExchangeDateKey  = dd.DateKey
       AND ex1.FromCurrencyID   = ts.TransCurrencyID
       AND ex1.ToCurrencyID     = le.AccountingCurrencyID
       AND ex1.ExchangeRateType = le.TransExchangeRateType;
