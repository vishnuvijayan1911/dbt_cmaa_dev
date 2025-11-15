{{ config(materialized='table', tags=['silver'], alias='salesagreementline_fact') }}

-- Source file: cma/cma/layers/_base/_silver/salesagreementline_f/salesagreementline_f.py
-- Root method: SalesagreementlineFact.salesagreementline_factdetail [SalesAgreementLine_FactDetail]
-- Inlined methods: SalesagreementlineFact.salesagreementline_factinvoicedquantity [SalesAgreementLine_FactInvoicedQuantity], SalesagreementlineFact.salesagreementline_factquantity [SalesAgreementLine_FactQuantity], SalesagreementlineFact.salesagreementline_factstage [SalesAgreementLine_FactStage], SalesagreementlineFact.salesagreementline_factline [SalesAgreementLine_FactLine], SalesagreementlineFact.salesagreementline_factdetail1 [SalesAgreementLine_FactDetail1]
-- external_table_name: SalesAgreementLine_FactDetail
-- schema_name: temp

WITH
salesagreementline_factinvoicedquantity AS (
    SELECT al.recid    AS RecID_AL
             , SUM(cit.qty) AS InvoicedQuantity

          FROM {{ ref('agreementline') }}                  al

         INNER JOIN {{ ref('agreementlinereleasedline') }} arl
            ON arl.agreementline = al.recid
           AND arl.isdeleted     = 0
         INNER JOIN {{ ref('sqldictionary') }}             sd
            ON sd.fieldid        = 0
           AND sd.tabid          = arl.referencerelationtype
           AND lower(sd.name)           = 'custinvoicetrans'
         INNER JOIN {{ ref('custinvoicetrans') }}          cit
            ON cit.recid        = arl.custinvoicetrans
         GROUP BY al.recid;
),
salesagreementline_factquantity AS (
    SELECT al.recid                    AS RecID_AL
             , SUM(sl.remainsalesfinancial) AS ReceivedQuantity
             , SUM(sl.remainsalesphysical)  AS ReleasedQuantity

          FROM {{ ref('agreementline') }}                  al
         INNER JOIN {{ ref('agreementlinereleasedline') }} arl
            ON arl.agreementline = al.recid
           AND arl.isdeleted     = 0
         INNER JOIN {{ ref('sqldictionary') }}             sd
            ON sd.fieldid        = 0
           AND sd.tabid          = arl.referencerelationtype
           AND lower(sd.name)           = 'salesline'
         INNER JOIN {{ ref('salesline') }}                 sl
            ON sl.dataareaid   = arl.saleslinedataareaid
           AND sl.inventtransid  = arl.saleslineinventtransid
         GROUP BY al.recid;
),
salesagreementline_factstage AS (
    SELECT DISTINCT
               ISNULL(NULLIF(sah.customerdataareaid, ''), al.inventdimdataareaid)                                     AS LegalEntityID
             , ah.agreementstate                                                                                     AS AgreementStateID
             , sah.custaccount                                                                                        AS CustomerAccount
             , al.itemid                                                                                             AS ItemID
             , id.inventsizeid                                                                                       AS ProductWidth
             , id.inventcolorid                                                                                      AS ProductLength
             , id.inventstyleid                                                                                      AS ProductColor
             , id.configid                                                                                           AS ProductConfig
             , id.inventsiteid                                                                                       AS siteid
             , id.inventlocationid                                                                                   AS warehouseid
             , alqc.productunitofmeasure                                                                               AS AgreementUnit
             , ah.defaultagreementlineeffectivedate                                                                  AS EffectiveDate
             , ah.defaultagreementlineexpirationdate                                                                 AS ExpirationDate
             , al.expirationdate                                                                                     AS LineExpirationDate
             , al.effectivedate                                                                                      AS LineEffectiveDate
             , CAST(CAST(ah.createddatetime AS Datetime) AT TIME ZONE 'UTC' AT TIME ZONE le.TimeZone AS DATE)        AS CreatedDate
             , ah.currency                                                                                           AS CurrencyID
             , alvc.commitedamount                                                                                     AS BaseAmount_TransCur
             , alqc.priceperunit                                                                                       AS BaseUnitPrice_TransCur
             , alqc.cmapriceuom                                                                                        AS pricingunit
             , alqc.cmatotalamount                                                                                     AS TotalAmount_TransCur
             , alqc.cmatotalprice                                                                                      AS totalprice_transcur
             , alqc.commitedquantity                                                                                   AS AgreementQuantity
             , tiq.InvoicedQuantity                                                                                  AS InvoicedQuantity
             , tq.ReceivedQuantity                                                                                   AS ReceivedQuantity
             , tq.ReleasedQuantity                                                                                   AS ReleasedQuantity
             , alqc.commitedquantity
               - (ISNULL(tiq.InvoicedQuantity, 0) + ISNULL(tq.ReceivedQuantity, 0) + ISNULL(tq.ReleasedQuantity, 0)) AS RemainingQuantity
             , al.recid                                                                                           AS _RecID
             , 1                                                                                                     AS _SourceID

          FROM {{ ref('agreementline') }}        al
          LEFT JOIN {{ ref('agreementlinequantitycommitment') }} alqc
              ON alqc.recid = al.recid
          LEFT JOIN {{ ref('agreementlinevolumecommitment') }} alvc
    		    ON alvc.recid = al.recid
         INNER JOIN {{ ref('agreementheader') }} ah
            ON ah.recid       = al.agreement
         LEFT JOIN {{ ref('salesagreementheader') }} sah
            ON sah.recid = ah.recid
         INNER JOIN silver.cma_LegalEntity     le
            ON le.LegalEntityID = ISNULL(NULLIF(sah.customerdataareaid, ''), al.inventdimdataareaid)
    		AND ISNULL(NULLIF(sah.customerdataareaid, ''), al.inventdimdataareaid) != ''
          LEFT JOIN {{ ref('inventdim') }}       id
            ON id.dataareaid   = ISNULL(NULLIF(sah.customerdataareaid, ''), al.inventdimdataareaid)
    		AND ISNULL(NULLIF(sah.customerdataareaid, ''), al.inventdimdataareaid) != ''
           AND id.inventdimid   = al.inventdimid
          LEFT JOIN salesagreementline_factinvoicedquantity   tiq
            ON tiq.RecID_AL     = al.recid
          LEFT JOIN salesagreementline_factquantity           tq
            ON tq.RecID_AL      = al.recid;
),
salesagreementline_factline AS (
    SELECT ts.LegalEntityID                                       AS LegalEntityID
             , ts.AgreementStateID                                    AS AgreementStateID
             , ts.CustomerAccount                                     AS CustomerAccount
             , ts.ItemID                                              AS ItemID
             , ts.ProductWidth                                        AS ProductWidth
             , ts.ProductLength                                       AS ProductLength
             , ts.ProductColor                                        AS ProductColor
             , ts.ProductConfig                                       AS ProductConfig
             , ts.siteid                                               AS siteid
             , ts.warehouseid                                          AS warehouseid
             , ts.AgreementUnit                                       AS AgreementUnit
             , ts.EffectiveDate                                       AS EffectiveDate
             , ts.ExpirationDate                                      AS ExpirationDate
             , ts.LineExpirationDate                                  AS LineExpirationDate
             , ts.LineEffectiveDate                                   AS LineEffectiveDate
             , ts.CreatedDate                                         AS CreatedDate
             , ts.BaseAmount_TransCur * ISNULL(ex.ExchangeRate, 1)    AS BaseAmount
             , ts.BaseAmount_TransCur                                 AS BaseAmount_TransCur
             , ts.BaseUnitPrice_TransCur * ISNULL(ex.ExchangeRate, 1) AS BaseUnitPrice
             , ts.BaseUnitPrice_TransCur                              AS BaseUnitPrice_TransCur
             , ts.pricingunit                                         AS pricingunit
             , ts.TotalAmount_TransCur * ISNULL(ex.ExchangeRate, 1)   AS TotalAmount
             , ts.TotalAmount_TransCur                                AS TotalAmount_TransCur
             , ts.totalprice_transcur * ISNULL (ex.exchangerate, 1)    AS totalprice
             , ts.totalprice_transcur                                  AS totalprice_transcur
             , ts.AgreementQuantity                                   AS AgreementQuantity
             , ts.InvoicedQuantity                                    AS InvoicedQuantity
             , ts.ReceivedQuantity                                    AS ReceivedQuantity
             , ts.ReleasedQuantity                                    AS ReleasedQuantity
             , ts.RemainingQuantity                                   AS RemainingQuantity
             , ts._RecID                                              AS _RecID
             , ts._SourceID                                           AS _SourceID
          FROM salesagreementline_factstage                     ts
         INNER JOIN silver.cma_Date              dd
            ON dd.Date             = ts.CreatedDate
         INNER JOIN silver.cma_LegalEntity       le
            ON le.LegalEntityID    = ts.LegalEntityID
          LEFT JOIN silver.cma_ExchangeRate_Fact ex
            ON ex.ExchangeDateKey  = dd.DateKey
           AND ex.FromCurrencyID   = ts.CurrencyID
           AND ex.ToCurrencyID     = le.AccountingCurrencyID
           AND ex.ExchangeRateType = le.TransExchangeRateType;
),
salesagreementline_factdetail1 AS (
    SELECT DISTINCT
               dsal.SalesAgreementLineKey AS SalesAgreementLineKey
             , das.AgreementStateKey      AS AgreementStateKey
             , du.UOMKey                  AS AgreementUOMKey
             , dc.CustomerKey             AS CustomerKey
             , ds.inventorysitekey          AS inventorysitekey
             , ISNULL(dp.ProductKey, -1)  AS ProductKey
             , du1.uomkey                   AS pricinguomkey
             , dd.DateKey                 AS EffectiveDateKey
             , dd1.DateKey                AS ExpirationDateKey
             , dd2.DateKey                AS LineEffectiveDateKey
             , dd3.DateKey                AS LineExpirationDateKey
             , le.LegalEntityKey          AS LegalEntityKey
             , dw.warehousekey              AS warehousekey
             , tl.AgreementQuantity       AS AgreementQuantity
             , tl.BaseAmount              AS BaseAmount
             , tl.BaseAmount_TransCur     AS BaseAmount_TransCur
             , tl.BaseUnitPrice           AS BaseUnitPrice
             , tl.BaseUnitPrice_TransCur  AS BaseUnitPrice_TransCur
             , tl.InvoicedQuantity        AS InvoicedQuantity
             , tl.ReceivedQuantity        AS ReceivedQuantity
             , tl.ReleasedQuantity        AS ReleasedQuantity
             , tl.RemainingQuantity       AS RemainingQuantity
             , tl.TotalAmount             AS TotalAmount
             , tl.TotalAmount_TransCur    AS TotalAmount_TransCur
             , tl.totalprice                AS totalprice
             , tl.totalprice_transcur       AS totalprice_transcur
             , tl._RecID                  AS _RecID
             , tl._SourceID               AS _SourceID
          FROM salesagreementline_factline                       tl
         INNER JOIN silver.cma_SalesAgreementLine dsal
            ON dsal._RecID          = tl._RecID
           AND dsal._SourceID       = 1
         INNER JOIN silver.cma_LegalEntity        le
            ON le.LegalEntityID     = tl.LegalEntityID
          LEFT JOIN silver.cma_AgreementState     das
            ON das.AgreementStateID = tl.AgreementStateID
          LEFT JOIN silver.cma_Customer           dc
            ON dc.LegalEntityID     = tl.LegalEntityID
           AND dc.CustomerAccount   = tl.CustomerAccount
          LEFT JOIN silver.cma_Product            dp
            ON dp.LegalEntityID     = tl.LegalEntityID
           AND dp.ItemID            = tl.ItemID
           AND dp.ProductWidth      = tl.ProductWidth
           AND dp.ProductLength     = tl.ProductLength
           AND dp.ProductColor      = tl.ProductColor
           AND dp.ProductConfig     = tl.ProductConfig
          LEFT JOIN silver.cma_Date               dd
            ON dd.Date              = tl.EffectiveDate
          LEFT JOIN silver.cma_Date               dd1
            ON dd1.Date             = tl.ExpirationDate
          LEFT JOIN silver.cma_Date               dd2
            ON dd2.Date              = tl.LineEffectiveDate
          LEFT JOIN silver.cma_Date               dd3
            ON dd3.Date             = tl.LineExpirationDate
          LEFT JOIN silver.cma_UOM                du
            ON du.UOM               = tl.AgreementUnit
          LEFT JOIN silver.cma_uom                  du1
            ON du1.uom              = tl.pricingunit
          LEFT JOIN silver.cma_warehouse            dw
            ON dw.legalentityid     = tl.legalentityid
           AND dw.warehouseid       = tl.warehouseid
          LEFT JOIN silver.cma_inventorysite        ds
            ON ds.legalentityid     = tl.legalentityid
           AND ds.inventorysiteid   = tl.siteid;
)
SELECT 
               CURRENT_TIMESTAMP                                                                      AS _CreatedDate
         , CURRENT_TIMESTAMP                                                                      AS _ModifiedDate 
         , tl.SalesAgreementLineKey                     AS SalesAgreementLineKey
         , tl.AgreementStateKey                         AS AgreementStateKey
         , tl.AgreementUOMKey                           AS AgreementUOMKey
         , tl.CustomerKey                               AS CustomerKey
         , tl.inventorysitekey                          AS inventorysitekey
         , tl.ProductKey                                AS ProductKey
         , tl.pricinguomkey                             AS pricinguomkey
         , tl.EffectiveDateKey                          AS EffectiveDateKey
         , tl.ExpirationDateKey                         AS ExpirationDateKey
         , tl.LineEffectiveDateKey                      AS LineEffectiveDateKey
         , tl.LineExpirationDateKey                     AS LineExpirationDateKey 
         , tl.LegalEntityKey                            AS LegalEntityKey
         , tl.warehousekey                              AS warehousekey
         , tl.AgreementQuantity                         AS AgreementQuantity
         , tl.AgreementQuantity * vuc.factor            AS AgreementQuantity_FT

         , tl.AgreementQuantity * vuc2.factor           AS AgreementQuantity_LB
         , ROUND(tl.AgreementQuantity * vuc3.factor, 0) AS AgreementQuantity_PC
         , tl.AgreementQuantity * vuc4.factor           AS AgreementQuantity_SQIN

         , tl.BaseAmount                                AS BaseAmount
         , tl.BaseAmount_TransCur                       AS BaseAmount_TransCur
         , tl.BaseUnitPrice                             AS BaseUnitPrice
         , tl.BaseUnitPrice_TransCur                    AS BaseUnitPrice_TransCur
         , tl.InvoicedQuantity                          AS InvoicedQuantity
         , tl.InvoicedQuantity * vuc.factor             AS InvoicedQuantity_FT

         , tl.InvoicedQuantity * vuc2.factor            AS InvoicedQuantity_LB
         , ROUND(tl.InvoicedQuantity * vuc3.factor, 0)  AS InvoicedQuantity_PC
         , tl.InvoicedQuantity * vuc4.factor            AS InvoicedQuantity_SQIN

         , tl.ReceivedQuantity                          AS ReceivedQuantity
         , tl.ReceivedQuantity * vuc.factor             AS ReceivedQuantity_FT

         , tl.ReceivedQuantity * vuc2.factor            AS ReceivedQuantity_LB
         , ROUND(tl.ReceivedQuantity * vuc3.factor, 0)  AS ReceivedQuantity_PC
         , tl.ReceivedQuantity * vuc4.factor            AS ReceivedQuantity_SQIN

         , tl.ReleasedQuantity                          AS ReleasedQuantity
         , tl.ReleasedQuantity * vuc.factor             AS ReleasedQuantity_FT

         , tl.ReleasedQuantity * vuc2.factor            AS ReleasedQuantity_LB
         , ROUND(tl.ReleasedQuantity * vuc3.factor, 0)  AS ReleasedQuantity_PC
         , tl.ReleasedQuantity * vuc4.factor            AS ReleasedQuantity_SQIN

         , tl.RemainingQuantity                         AS RemainingQuantity
         , tl.RemainingQuantity * vuc.factor            AS RemainingQuantity_FT

         , tl.RemainingQuantity * vuc2.factor           AS RemainingQuantity_LB
         , ROUND(tl.RemainingQuantity * vuc3.factor, 0) AS RemainingQuantity_PC
         , tl.RemainingQuantity * vuc4.factor           AS RemainingQuantity_SQIN

         , tl.TotalAmount                               AS TotalAmount
         , tl.TotalAmount_TransCur                      AS TotalAmount_TransCur
         , tl.totalprice                                AS totalprice
         , tl.totalprice_transcur                       AS totalprice_transcur
         , tl._RecID                                    AS _RecID
         , tl._SourceID                                 AS _SourceID

      FROM salesagreementline_factdetail1                tl
      LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc
        ON vuc.legalentitykey  = tl.LegalEntityKey
       AND vuc.productkey      = tl.ProductKey
       AND vuc.fromuomkey      = tl.AgreementUOMKey
    -- AND vuc.touom           = 'ft'





      LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc2
        ON vuc2.legalentitykey = tl.LegalEntityKey
       AND vuc2.productkey     = tl.ProductKey
       AND vuc2.fromuomkey     = tl.AgreementUOMKey
    -- AND vuc2.touom          = 'lb'
      LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc3
        ON vuc3.legalentitykey = tl.LegalEntityKey
       AND vuc3.productkey     = tl.ProductKey
       AND vuc3.fromuomkey     = tl.AgreementUOMKey
    -- AND vuc3.touom          = 'pc'
      LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc4
        ON vuc4.legalentitykey = tl.LegalEntityKey
       AND vuc4.productkey     = tl.ProductKey
       AND vuc4.fromuomkey     = tl.AgreementUOMKey
    -- AND vuc4.touom          = 'sqin'
