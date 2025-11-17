{{ config(materialized='table', tags=['silver'], alias='purchaseorderlinecharge_fact') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseorderlinecharge_f/purchaseorderlinecharge_f.py
-- Root method: PurchaseorderlinechargeFact.purchaseorderlinecharge_factdetail [PurchaseOrderLineCharge_FactDetail]
-- Inlined methods: PurchaseorderlinechargeFact.purchaseorderlinecharge_factproduct [PurchaseOrderLineCharge_FactProduct], PurchaseorderlinechargeFact.purchaseorderlinecharge_factpurchasecharge1 [PurchaseOrderLineCharge_FactpurchaseCharge1], PurchaseorderlinechargeFact.purchaseorderlinecharge_factpurchasecharge2 [PurchaseOrderLineCharge_FactpurchaseCharge2], PurchaseorderlinechargeFact.purchaseorderlinecharge_factpurchasecharge3 [PurchaseOrderLineCharge_FactpurchaseCharge3], PurchaseorderlinechargeFact.purchaseorderlinecharge_factpurchasecharge [PurchaseOrderLineCharge_FactpurchaseCharge], PurchaseorderlinechargeFact.purchaseorderlinecharge_factstage [PurchaseOrderLineCharge_FactStage], PurchaseorderlinechargeFact.purchaseorderlinecharge_factcharge [PurchaseOrderLineCharge_FactCharge]
-- external_table_name: PurchaseOrderLineCharge_FactDetail
-- schema_name: temp

WITH
purchaseorderlinecharge_factproduct AS (
    SELECT dp.LegalEntityID
             , dp.ItemID
             , dp.ProductLength
             , dp.ProductWidth
             , dp.ProductColor
             , dp.ProductConfig
             , dp.ProductKey

          FROM {{ ref('product_d') }} dp;
),
purchaseorderlinecharge_factpurchasecharge1 AS (
    select 
              pl.currencycode                                as currencycode
    		 , mt.markupcategory
    		 , mt.cmapriceuom
    		 , mt.value
    		 , pl.purchqty
    		 , pl.lineamount
    		 ,pl.dataareaid
    		 ,pl.inventdimid
    		 , pl.itemid
    		 , pl.purchunit
             , pl.recid                                      as recid_pl
             , mt.recid                                      as recid
              from {{ ref('markuptrans') }}               mt
         inner join {{ ref('purchline') }}            pl
            on pl.recid          = mt.transrecid
),
purchaseorderlinecharge_factpurchasecharge2 AS (
    select 
              p1.currencycode                               
    		 , p1.markupcategory
    		 ,  p1.cmapriceuom
    		 , p1.value
    		 , p1.purchqty
    		 , p1.lineamount
    		 , id.inventcolorid
    		 , id.inventsizeid
    		 , id.inventstyleid
    		 , id.configid
    		 , p1.dataareaid
    		 , p1.inventdimid
    		 , p1.itemid
    		 , p1.purchunit
             , p1.recid_pl                                     
             , p1.recid                                     
          from purchaseorderlinecharge_factpurchasecharge1               p1
         inner join {{ ref('inventdim') }}            id
            on id.dataareaid     = p1.dataareaid
           and id.inventdimid     = p1.inventdimid
),
purchaseorderlinecharge_factpurchasecharge3 AS (
    select 
                    p1.currencycode                               
    		 , p1.markupcategory
    		 ,  p1.cmapriceuom
    		 , p1.value
    		 , p1.purchqty
    		 , p1.lineamount
    		 , p1.inventcolorid
    		 , p1.inventsizeid
    		 , p1.inventstyleid
    		 , p1.configid
    		 , p1.dataareaid
    		 , p1.inventdimid
    		 , p1.itemid
    		 , p1.purchunit
    		 , tp.legalentityid
    		 , tp.productkey
             , p1.recid_pl                                     
             , p1.recid
          from purchaseorderlinecharge_factpurchasecharge2               p1
         inner join purchaseorderlinecharge_factproduct                 tp
          on tp.legalentityid   = p1.dataareaid
           and tp.itemid          = p1.itemid
           and tp.productlength   = p1.inventcolorid
           and tp.productwidth    = p1.inventsizeid
           and tp.productcolor    = p1.inventstyleid
           and tp.productconfig   = p1.configid
),
purchaseorderlinecharge_factpurchasecharge AS (
    select 
                    case when p1.markupcategory = 0
                    then p1.value
                    when p1.markupcategory = 1
                    then p1.purchqty * ISNULL(vuc1.factor, 0)

                         * p1.value
                    when p1.markupcategory = 2
                    then (p1.value * p1.lineamount) / 100 end as calculatedamount
             , p1.currencycode                                as currencycode
             , p1.recid_pl                                      
             , p1.recid                                      
          from purchaseorderlinecharge_factpurchasecharge3               p1
           left join {{ ref('vwuomconversion') }} vuc1
            on vuc1.productkey    = p1.productkey
           and vuc1.fromuom       = p1.purchunit
           and vuc1.touom         = p1.cmapriceuom
           and vuc1.legalentityid = p1.legalentityid
),
purchaseorderlinecharge_factstage AS (
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
             , t._RecID1
             , t._RecID

          FROM (   SELECT mt.currencycode                                                                             AS ChargeCurrencyID
                        , mt.dataareaid                                                                              AS LegalEntityID
                        , mt.markupcategory                                                                           AS MarkupCategoryID
                        , mt.markupcode                                                                               AS Code
                        , mt.moduletype                                                                               AS ModuleType
                        , mu.custtype                                                                                 AS ChargeTypeID
                        , pl.currencycode                                                                             AS TransCurrencyID
                        , 0                                                                                           AS IncludedCharge
                        , 0                                                                                           AS AdditionalCharge
                        , 0                                                                                           AS NonBillableCharge
                        , CASE WHEN mu.custtype <> 1
                               THEN mt.calculatedamount / (COUNT(pl.recid) OVER (PARTITION BY mt.recid)) ELSE 0 END AS BillableHeaderCharge
                        , CASE WHEN mu.custtype = 1
                               THEN mt.calculatedamount / (COUNT(pl.recid) OVER (PARTITION BY mt.recid)) ELSE 0 END AS NonBillableHeaderCharge
                        , mt.taxamount                                                                                AS TaxAmount
                        , 0                                                                                           AS TotalCharge
                        , mt.cmapriceuom                                                                       AS PriceUnit
                        , 0                                                                                           AS IncludeInTotalPrice
                        , 0                                                                                           AS PrintCharges
                        , CAST(mt.transdate AS DATE)                                                                  AS TransDate
                        , pl.recid                                                                                   AS _RecID1
                        , mt.recid                                                                                   AS _RecID
                     FROM {{ ref('markuptrans') }}        mt
                    INNER JOIN {{ ref('markuptable') }}   mu
                       ON mu.dataareaid = mt.dataareaid
                      AND mu.markupcode  = mt.markupcode
                      AND mu.moduletype  = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }} sd
                       ON sd.fieldid     = 0
                      AND sd.tabid       = mt.transtableid
                      AND sd.name        = 'PurchTable'
                    INNER JOIN {{ ref('purchtable') }}    ph
                       ON ph.recid      = mt.transrecid
                    INNER JOIN {{ ref('purchline') }}     pl
                       ON pl.dataareaid = ph.dataareaid
                      AND pl.purchid     = ph.purchid
                   UNION
                   SELECT mt.currencycode                                                                     AS ChargeCurrencyID
                        , mt.dataareaid                                                                      AS LegalEntityID
                        , mt.markupcategory                                                                   AS MarkupCategoryID
                        , mt.markupcode                                                                       AS Code
                        , mt.moduletype                                                                       AS ModuleType
                        , mu.custtype                                                                         AS ChargeTypeID
                        , pl.currencycode                                                                     AS TransCurrencyID
                        , CASE WHEN mt.cmarollup = 1 AND mu.custtype <> 1 THEN pl.calculatedamount ELSE 0 END AS IncludedCharge
                        , CASE WHEN mt.cmarollup = 0 AND mu.custtype <> 1 THEN pl.calculatedamount ELSE 0 END AS AdditionalCharge
                        , CASE WHEN mu.custtype = 1 THEN pl.calculatedamount ELSE 0 END                       AS NonBillableCharge
                        , 0                                                                                   AS BillableHeaderCharge
                        , 0                                                                                   AS NonBillableHeaderCharge
                        , mt.taxamount                                                                        AS TaxAmount
                        , pl.calculatedamount                                                                 AS TotalCharge
                        , mt.cmapriceuom                                                               AS PriceUnit
                        , mt.cmarollup                                                                        AS IncludeInTotalPrice
                        , mt.cmatoprint                                                                       AS PrintCharges
                        , CAST(mt.transdate AS DATE)                                                          AS TransDate
                        , pl.recid_pl                                                                         AS _RecID1
                        , mt.recid                                                                           AS _RecID
                     FROM {{ ref('markuptrans') }}        mt
                    INNER JOIN {{ ref('markuptable') }}   mu
                       ON mu.dataareaid = mt.dataareaid
                      AND mu.markupcode  = mt.markupcode
                      AND mu.moduletype  = mt.moduletype
                    INNER JOIN {{ ref('sqldictionary') }} sd
                       ON sd.fieldid     = 0
                      AND sd.tabid       = mt.transtableid
                      AND sd.name        = 'PURCHLINE'
                    INNER JOIN purchaseorderlinecharge_factpurchasecharge   pl
                       ON pl.recid       = mt.recid) AS t;
),
purchaseorderlinecharge_factcharge AS (
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
             , t.PriceUnit                                      AS PriceUnit
             , t.IncludeInTotalPrice                            AS IncludeInTotalPrice
             , t.PrintCharges                                   AS PrintCharges
             , t.TransDate                                      AS TransDate
             , t._RecID1                                        AS _RecID1
             , t._RecID                                         AS _RecID

          FROM purchaseorderlinecharge_factstage                     t
         INNER JOIN {{ ref('legalentity_d') }}       le
            ON le.LegalEntityID    = t.LegalEntityID
         INNER JOIN {{ ref('date_d') }}              dd
            ON dd.Date             = t.TransDate
          LEFT JOIN {{ ref('exchangerate_f') }} ex
            ON ex.ExchangeDateKey  = dd.DateKey
           AND ex.FromCurrencyID   = t.ChargeCurrencyID
           AND ex.ToCurrencyID     = t.TransCurrencyID
           AND ex.ExchangeRateType = le.TransExchangeRateType;
)
SELECT ROW_NUMBER() OVER (ORDER BY ts._RecID) AS PurchaseOrderLineChargeKey
         , dc.ChargeCodeKey                                            AS ChargeCodeKey
         , dcc.ChargeCategoryKey                                       AS ChargeCategoryKey
         , cur.CurrencyKey                                             AS ChargeCurrencyKey
         , ct.ChargeTypeKey                                            AS ChargeTypeKey
         , le.LegalEntityKey                                           AS LegalEntityKey
         , du.UOMKey                                                   AS PricingUOMKey
         , dpol.PurchaseOrderLineKey                                   AS PurchaseOrderLineKey
         , dd.DateKey                                                  AS TransDateKey
         , ts.AdditionalCharge_TransCur * ISNULL(ex1.ExchangeRate, 1)  AS AdditionalCharge
         , ts.AdditionalCharge_TransCur                                AS AdditionalCharge_TransCur
         , ts.IncludedCharge_TransCur * ISNULL(ex1.ExchangeRate, 1)    AS IncludedCharge
         , ts.IncludedCharge_TransCur                                  AS IncludedCharge_TransCur
         , ts.IncludeInTotalPrice                                      AS IncludeInTotalPrice
         , ts.NonBillableCharge_TransCur * ISNULL(ex1.ExchangeRate, 1) AS NonBillableCharge
         , ts.NonBillableCharge_TransCur                               AS NonBillableCharge_TransCur
         , ts.PrintCharges * ISNULL(ex1.ExchangeRate, 1)               AS PrintCharges
         , ts.TaxAmount_TransCur * ISNULL(ex1.ExchangeRate, 1)         AS TaxAmount
         , ts.TaxAmount_TransCur                                       AS TaxAmount_TransCur
         , ts.TotalCharges_TransCur * ISNULL(ex1.ExchangeRate, 1)      AS TotalCharges
         , ts.TotalCharges_TransCur                                    AS TotalCharges_TransCur
         , ts._RecID                                                   AS _RecID1
         , ts._RecID1                                                  AS _RecID2
         , 1                                                           AS _SourceID
         , CURRENT_TIMESTAMP AS _CreatedDate
         , CURRENT_TIMESTAMP AS _ModifiedDate

      FROM purchaseorderlinecharge_factcharge                    ts
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
      LEFT JOIN {{ ref('purchaseorderline_d') }} dpol
        ON dpol._RecID          = ts._RecID1
       AND dpol._SourceID       = 1
      LEFT JOIN {{ ref('chargetype_d') }}        ct
        ON ct.ChargeTypeID      = ts.ChargeTypeID
      LEFT JOIN {{ ref('uom_d') }}               du
        ON du.UOM               = ts.PriceUnit
      LEFT JOIN {{ ref('exchangerate_f') }} ex1
        ON ex1.ExchangeDateKey  = dd.DateKey
       AND ex1.FromCurrencyID   = ts.TransCurrencyID
       AND ex1.ToCurrencyID     = le.AccountingCurrencyID
       AND ex1.ExchangeRateType = le.TransExchangeRateType;
