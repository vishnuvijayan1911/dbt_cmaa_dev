{{ config(materialized='table', tags=['silver'], alias='salesorderlinecharge_fact') }}

-- Source file: cma/cma/layers/_base/_silver/salesorderlinecharge_f/salesorderlinecharge_f.py
-- Root method: SalesorderlinechargeFact.salesorderlinecharge_factdetail [SalesOrderLineCharge_FactDetail]
-- Inlined methods: SalesorderlinechargeFact.salesorderlinechargefactproduct [SalesOrderLineChargeFactProduct], SalesorderlinechargeFact.salesorderlinechargesales1 [SalesOrderLineChargeSales1], SalesorderlinechargeFact.salesorderlinechargesales2 [SalesOrderLineChargeSales2], SalesorderlinechargeFact.salesorderlinechargesales3 [SalesOrderLineChargeSales3], SalesorderlinechargeFact.salesorderlinechargesales [SalesOrderLineChargeSales], SalesorderlinechargeFact.salesorderlinechargefactproductstage [SalesOrderLineChargeFactProductStage], SalesorderlinechargeFact.salesorderlineproductcharge [SalesOrderLineProductCharge]
-- external_table_name: SalesOrderLineCharge_FactDetail
-- schema_name: temp

WITH
salesorderlinechargefactproduct AS (
    SELECT dp.LegalEntityID
                , dp.ItemID
                , dp.ProductLength
                , dp.ProductWidth
                , dp.ProductColor
                , dp.ProductConfig
                , dp.ProductKey
             FROM {{ ref('product_d') }} dp;
),
salesorderlinechargesales1 AS (
    select 
              sl.currencycode                                as currencycode
    		 , mt.markupcategory
    		 , mt.cmapriceuom
    		 , mt.value
    		 , sl.salesqty
    		 , sl.lineamount
    		 ,sl.dataareaid
    		 ,sl.inventdimid
    		 , sl.itemid
    		 , sl.salesunit
             , sl.recid                                      as recid_sl
             , mt.recid                                      as recid
          from {{ ref('markuptrans') }}               mt
         inner join {{ ref('sqldictionary') }}        sd
            on sd.fieldid         = 0
           and sd.tabid           = mt.transtableid
           and sd.name            = 'salesline'
    	inner join {{ ref('salesline') }}            sl
            on sl.recid          = mt.transrecid
),
salesorderlinechargesales2 AS (
    select 
              s1.currencycode                                as currencycode
    		 , s1.markupcategory
    		 ,  s1.cmapriceuom
    		 , s1.value
    		 , s1.salesqty
    		 , s1.lineamount
    		 , id.inventcolorid
    		 , id.inventsizeid
    		 , id.inventstyleid
    		 , id.configid
    		 , s1.dataareaid
    		 , s1.inventdimid
    		 , s1.itemid
    		 , s1.salesunit
             , s1.recid_sl                                      as recid_sl
             , s1.recid                                      as recid
          from salesorderlinechargesales1               s1
         inner join {{ ref('inventdim') }}            id
            on id.dataareaid     = s1.dataareaid
           and id.inventdimid     = s1.inventdimid
),
salesorderlinechargesales3 AS (
    select 
              s1.currencycode                                as currencycode
    		 , s1.markupcategory
    		 ,  s1.cmapriceuom
    		 , s1.value
    		 , s1.salesqty
    		 , s1.lineamount
    		 , s1.inventcolorid
    		 , s1.inventsizeid
    		 , s1.inventstyleid
    		 , s1.configid
    		 , s1.dataareaid
    		 , s1.inventdimid
    		 , tp.productkey
    		 , s1.salesunit
    		 , tp.legalentityid
             , s1.recid_sl                                      as recid_sl
             , s1.recid                                      as recid
          from salesorderlinechargesales2               s1
         inner join salesorderlinechargefactproduct                 tp
          on tp.legalentityid   = s1.dataareaid
           and tp.itemid          = s1.itemid
           and tp.productlength   = s1.inventcolorid
           and tp.productwidth    = s1.inventsizeid
           and tp.productcolor    = s1.inventstyleid
           and tp.productconfig   = s1.configid
),
salesorderlinechargesales AS (
    select 
            case when s1.markupcategory = 0
                   then s1.value
                   when s1.markupcategory = 1
                   then s1.salesqty * ISNULL(vuc1.factor, 0)

                        * s1.value
                   when s1.markupcategory = 2
                   then (s1.value * s1.lineamount) / 100 end as calculatedamount
            , s1.currencycode                                as currencycode
            , s1.recid_sl                                      as recid_sl
            , s1.recid                                      as recid
         from salesorderlinechargesales3               s1
        left join {{ ref('vwuomconversion') }} vuc1
           on vuc1.productkey    = s1.productkey
          and vuc1.fromuom       = s1.salesunit
          and vuc1.touom         = s1.cmapriceuom
          and vuc1.legalentityid = s1.legalentityid
),
salesorderlinechargefactproductstage AS (
    SELECT t.ChargeCurrencyID
             , t.LegalEntityID
             , t.MarkupCategoryID
             , t.Code
             , t.ModuleType
             , t.ChargeTypeID
             , t.TransCurrencyID
             , t.IncludedCharge
             , t.AdditionalCharge + t.BillableHeaderCharge     AS AdditionalCharge
             , t.NonBillableCharge + t.NonBillableHeaderCharge AS NonBillableCharge
             , t.TaxAmount
             , t.TotalCharge
             , t.PriceUnit
             , t.IncludeInTotalPrice
             , t.PrintCharges
             , t.TransDate
             , t._SourceDate
             , t._RecID1
             , t._RecID
          FROM (   SELECT mt.currencycode                                                                                       AS ChargeCurrencyID
                        , mt.dataareaid                                                                                        AS LegalEntityID
                        , mt.markupcategory                                                                                     AS MarkupCategoryID
                        , mt.markupcode                                                                                         AS Code
                        , mt.moduletype                                                                                         AS ModuleType
                        , mu.custtype                                                                                           AS ChargeTypeID
                        , sl.currencycode                                                                                       AS TransCurrencyID
                        , 0                                                                                                     AS IncludedCharge
                        , 0                                                                                                     AS AdditionalCharge
                        , 0                                                                                                     AS NonBillableCharge
                        , CASE WHEN mu.custtype <> 1 THEN mt.value / (COUNT(sl.recid) OVER (PARTITION BY mt.recid)) ELSE
                                                                                                                      0 END     AS BillableHeaderCharge
                        , CASE WHEN mu.custtype = 1 THEN mt.value / (COUNT(sl.recid) OVER (PARTITION BY mt.recid)) ELSE 0 END AS NonBillableHeaderCharge
                        , mt.taxamount                                                                                          AS TaxAmount
                        , 0                                                                                                     AS TotalCharge
                        , mt.cmapriceuom                                                                               AS PriceUnit
                        , 0                                                                                                     AS IncludeInTotalPrice
                        , 0                                                                                                     AS PrintCharges
                        , CAST(mt.transdate AS DATE)                                                                            AS TransDate
                        , mt.modifieddatetime                                                                                  AS _SourceDate
                        , sl.recid                                                                                             AS _RecID1
                        , mt.recid                                                                                             AS _RecID
                     FROM {{ ref('markuptrans') }}        mt
                    INNER JOIN {{ ref('markuptable') }}   mu
                       ON mu.dataareaid = mt.dataareaid
                      AND mu.markupcode  = mt.markupcode
                      AND mu.moduletype  = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }} sd
                       ON sd.fieldid     = 0
                      AND sd.tabid       = mt.transtableid
                      AND sd.name        = 'SALESTABLE'
                    INNER JOIN {{ ref('salestable') }}    st
                       ON st.recid      = mt.transrecid
                    INNER JOIN {{ ref('salesline') }}     sl
                       ON sl.dataareaid = st.dataareaid
                      AND sl.salesid     = st.salesid
                   UNION
                   SELECT mt.currencycode                                                                     AS ChargeCurrencyID
                        , mt.dataareaid                                                                      AS LegalEntityID
                        , mt.markupcategory                                                                   AS MarkupCategoryID
                        , mt.markupcode                                                                       AS Code
                        , mt.moduletype                                                                       AS ModuleType
                        , mu.custtype                                                                         AS ChargeTypeID
                        , sl.currencycode                                                                     AS TransCurrencyID
                        , CASE WHEN mt.cmarollup = 1 AND mu.custtype <> 1 THEN sl.calculatedamount ELSE 0 END AS IncludedCharge
                        , CASE WHEN mt.cmarollup = 0 AND mu.custtype <> 1 THEN sl.calculatedamount ELSE 0 END AS AdditionalCharge
                        , CASE WHEN mu.custtype = 1 THEN sl.calculatedamount ELSE 0 END                       AS NonBillableCharge
                        , 0                                                                                   AS BillableHeaderCharge
                        , 0                                                                                   AS NonBillableHeaderCharge
                        , mt.taxamount                                                                        AS TaxAmount
                        , sl.calculatedamount                                                                 AS TotalCharge
                        , mt.cmapriceuom                                                             AS PriceUnit
                        , mt.cmarollup                                                                        AS IncludeInTotalPrice
                        , mt.cmatoprint                                                                       AS PrintCharges
                        , CAST(mt.transdate AS DATE)                                                          AS TransDate
                        , mt.modifieddatetime                                                                AS _SourceDate
                        , sl.recid_sl                                                                         AS _RecID1
                        , mt.recid                                                                           AS _RecID
                     FROM {{ ref('markuptrans') }}        mt
                    INNER JOIN {{ ref('markuptable') }}   mu
                       ON mu.dataareaid = mt.dataareaid
                      AND mu.markupcode  = mt.markupcode
                      AND mu.moduletype  = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }} sd
                       ON sd.fieldid     = 0
                      AND sd.tabid       = mt.transtableid
                      AND sd.name        = 'SalesLine'
                    INNER JOIN salesorderlinechargesales      sl
                       ON sl.recid       = mt.recid) AS t;
),
salesorderlineproductcharge AS (
    SELECT t.ChargeCurrencyID                               AS ChargeCurrencyID
             , t.LegalEntityID                                  AS LegalEntityID
             , t.MarkupCategoryID                               AS MarkupCategoryID
             , t.Code                                           AS Code
             , t.ModuleType                                     AS ModuleType
             , t.ChargeTypeID                                   AS ChargeTypeID
             , t.TransCurrencyID                                AS TransCurrencyID
             , t.IncludedCharge * ISNULL(ex.ExchangeRate, 1)    AS IncludedCharge_TransCur
             , t.AdditionalCharge * ISNULL(ex.ExchangeRate, 1)  AS AdditionalCharge_TransCur
             , t.NonBillableCharge * ISNULL(ex.ExchangeRate, 1) AS NonBillableCharge_TransCur
             , t.TaxAmount * ISNULL(ex.ExchangeRate, 1)         AS TaxAmount_TransCur
             , t.TotalCharge * ISNULL(ex.ExchangeRate, 1)       AS TotalCharges_TransCur
             , t.PriceUnit
             , t.IncludeInTotalPrice
             , t.PrintCharges
             , t.TransDate
             , t._SourceDate                                    AS _SourceDate
             , t._RecID1
             , t._RecID
          FROM salesorderlinechargefactproductstage                     t
         INNER JOIN {{ ref('legalentity_d') }}       le
            ON le.LegalEntityID    = t.LegalEntityID
          LEFT JOIN {{ ref('date_d') }}              dd
            ON dd.Date             = t.TransDate
          LEFT JOIN {{ ref('exchangerate_f') }} ex
            ON ex.ExchangeDateKey  = dd.DateKey
           AND ex.FromCurrencyID   = t.ChargeCurrencyID
           AND ex.ToCurrencyID     = t.TransCurrencyID
           AND ex.ExchangeRateType = le.TransExchangeRateType;
)
SELECT ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._RecID1) AS SalesOrderLineChargeKey
         ,dc.ChargeCodeKey                                            AS ChargeCodeKey
         , dcc.ChargeCategoryKey                                       AS ChargeCategoryKey
         , cur.CurrencyKey                                             AS ChargeCurrencyKey
         , ct.ChargeTypeKey                                            AS ChargeTypeKey
         , le.LegalEntityKey                                           AS LegalEntityKey
         , du.UOMKey                                                   AS PricingUOMKey
         , dsol.SalesOrderLineKey                                      AS SalesOrderLineKey
         , dd.DateKey                                                  AS TransDateKey
         , ts.AdditionalCharge_TransCur * ISNULL(ex1.ExchangeRate, 1)  AS AdditionalCharge
         , ts.AdditionalCharge_TransCur                                AS AdditionalCharge_TransCur
         , ts.IncludedCharge_TransCur * ISNULL(ex1.ExchangeRate, 1)    AS IncludedCharge
         , ts.IncludedCharge_TransCur                                  AS IncludedCharge_TransCur
         , ts.IncludeInTotalPrice                                      AS IncludeInTotalPrice
         , ts.NonBillableCharge_TransCur * ISNULL(ex1.ExchangeRate, 1) AS NonBillableCharge
         , ts.NonBillableCharge_TransCur                               AS NonBillableCharge_TransCur
         , ts.PrintCharges                                             AS PrintCharges
         , ts.TaxAmount_TransCur * ISNULL(ex1.ExchangeRate, 1)         AS TaxAmount
         , ts.TaxAmount_TransCur                                       AS TaxAmount_TransCur
         , ts.TotalCharges_TransCur * ISNULL(ex1.ExchangeRate, 1)      AS TotalCharges
         , ts.TotalCharges_TransCur                                    AS TotalCharges_TransCur
         , ts._SourceDate                                              AS _SourceDate
         , ts._RecID                                                   AS _RecID1
         , ts._RecID1                                                  AS _RecID2
         , 1                                                           AS _SourceID
         ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))  AS  _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
      FROM salesorderlineproductcharge                   ts
     INNER JOIN {{ ref('legalentity_d') }}       le
        ON le.LegalEntityID     = ts.LegalEntityID
      LEFT JOIN {{ ref('chargecode_d') }}        dc
        ON dc.LegalEntityID     = ts.LegalEntityID
       AND dc.ModuleTypeID      = ts.ModuleType
       AND dc.ChargeCode        = ts.Code
      LEFT JOIN {{ ref('chargecategory_d') }}    dcc
        ON dcc.ChargeCategoryID = ts.MarkupCategoryID
      LEFT JOIN {{ ref('date_d') }}              dd
        ON dd.Date              = ts.TransDate
      LEFT JOIN {{ ref('currency_d') }}          cur
        ON cur.CurrencyID       = ts.ChargeCurrencyID
      LEFT JOIN {{ ref('salesorderline_d') }}    dsol
        ON dsol._RecID          = ts._RecID1
       AND dsol._SourceID       = 1
      LEFT JOIN {{ ref('chargetype_d') }}        ct
        ON ct.ChargeTypeID      = ts.ChargeTypeID
      LEFT JOIN {{ ref('uom_d') }}               du
        ON du.UOM               = ts.PriceUnit
      LEFT JOIN {{ ref('exchangerate_f') }} ex1
        ON ex1.ExchangeDateKey  = dd.DateKey
       AND ex1.FromCurrencyID   = ts.TransCurrencyID
       AND ex1.ToCurrencyID     = le.AccountingCurrencyID
       AND ex1.ExchangeRateType = le.TransExchangeRateType;
