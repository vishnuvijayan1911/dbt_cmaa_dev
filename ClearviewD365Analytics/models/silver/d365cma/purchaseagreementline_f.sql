{{ config(materialized='table', tags=['silver'], alias='purchaseagreementline_fact') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseagreementline_f/purchaseagreementline_f.py
-- Root method: PurchaseagreementlineFact.purchaseagreementline_factdetail [PurchaseAgreementLine_FactDetail]
-- Inlined methods: PurchaseagreementlineFact.purchaseagreementline_factinvoicedquantity [PurchaseAgreementLine_FactInvoicedQuantity], PurchaseagreementlineFact.purchaseagreementline_factquantity [PurchaseAgreementLine_FactQuantity], PurchaseagreementlineFact.purchaseagreementline_factstage [PurchaseAgreementLine_FactStage], PurchaseagreementlineFact.purchaseagreementline_factline [PurchaseAgreementLine_FactLine], PurchaseagreementlineFact.purchaseagreementline_factdetail1 [PurchaseAgreementLine_FactDetail1]
-- external_table_name: PurchaseAgreementLine_FactDetail
-- schema_name: temp

WITH
purchaseagreementline_factinvoicedquantity AS (
    SELECT al.recid    AS RecID_AL
             , SUM(vit.qty) AS InvoicedQuantity
          FROM {{ ref('agreementline') }}                  al
         INNER JOIN {{ ref('agreementlinereleasedline') }} arl
            ON arl.agreementline = al.recid
           AND arl.isdeleted     = 0
         INNER JOIN {{ ref('sqldictionary') }}             sd
            ON sd.fieldid        = 0
           AND sd.tabid          = arl.referencerelationtype
           AND sd.name           = 'VENDINVOICETRANS'
         INNER JOIN {{ ref('vendinvoicetrans') }}          vit
            ON vit.recid        = arl.vendinvoicetrans
         GROUP BY al.recid;
),
purchaseagreementline_factquantity AS (
    SELECT al.recid                    AS RecID_AL
             , SUM(pl.remainpurchfinancial) AS ReceivedQuantity
             , SUM(pl.remainpurchphysical)  AS ReleasedQuantity

          FROM {{ ref('agreementline') }}                  al
         INNER JOIN {{ ref('agreementlinereleasedline') }} arl
            ON arl.agreementline = al.recid
           AND arl.isdeleted     = 0
         INNER JOIN {{ ref('sqldictionary') }}             sd
            ON sd.fieldid        = 0
           AND sd.tabid          = arl.referencerelationtype
           AND sd.name           = 'PurchLine'
         INNER JOIN {{ ref('purchline') }}                 pl
            ON pl.dataareaid    = arl.purchlinedataareaid
           AND pl.inventtransid  = arl.purchlineinventtransid
         GROUP BY al.recid;
),
purchaseagreementline_factstage AS (
    SELECT DISTINCT
               pah.vendordataareaid                                                                      AS LegalEntityID

              , NULL                                                                        AS AgreementStateID
             , pah.vendaccount                                                                           AS VendorAccount
             , al.itemid                                                                                AS ItemID
             , id.inventsizeid                                                                          AS ProductWidth
             , id.inventcolorid                                                                         AS ProductLength
             , id.inventstyleid                                                                         AS ProductColor
             , id.configid                                                                              AS ProductConfig
             , alqc.productunitofmeasure                                                                  AS AgreementUnit


             , ISNULL(CONVERT(VARCHAR(30),NULL,121),'')                                                                                    AS EffectiveDate
             , ISNULL(CONVERT(VARCHAR(30),NULL,121),'')                                                                                     AS ExpirationDate

              , ISNULL(CONVERT(VARCHAR(30),NULL,121),'')                                                AS CreatedDate   

              , ''                                                                                     AS CurrencyID  
             , alvc.commitedamount                                                                        AS BaseAmount_TransCur
             , alqc.priceperunit                                                                          AS BaseUnitPrice_TransCur
             , alqc.cmatotalamount                                                                        AS TotalAmount_TransCur
             , alqc.commitedquantity                                                                      AS AgreementQuantity
             , tiq.InvoicedQuantity                                                                     AS InvoicedQuantity
             , tq.ReceivedQuantity                                                                      AS ReceivedQuantity
             , tq.ReleasedQuantity                                                                      AS ReleasedQuantity
             , alqc.commitedquantity - (tiq.InvoicedQuantity + tq.ReceivedQuantity + tq.ReleasedQuantity) AS RemainingQuantity
             , al.recid                                                                                AS _RecID
             , 1                                                                                        AS _SourceID


            FROM {{ ref('agreementline') }}             al
            LEFT JOIN {{ ref('agreementlinequantitycommitment') }} alqc
              ON alqc.recid = al.recid
         LEFT JOIN {{ ref('agreementlinevolumecommitment') }} alvc
    		   ON alvc.recid = al.recid
         INNER JOIN {{ ref('agreementheader') }} ah
           ON ah.recid        = al.agreement
         LEFT JOIN {{ ref('purchagreementheader') }} pah
          ON pah.recid = ah.recid
        LEFT OUTER JOIN {{ ref('dimensionattributevalueset') }} T2 
           ON(( ah.defaultdimension  =  T2.recid)  
           AND ( ah.partition  =  T2.partition)) 
         INNER JOIN {{ ref('legalentity_d') }}          le
            ON le.LegalEntityID = al.inventdimdataareaid 
          LEFT JOIN {{ ref('inventdim') }}            id
            ON id.dataareaid   = al.inventdimdataareaid
           AND id.inventdimid   = al.inventdimid
          LEFT JOIN purchaseagreementline_factinvoicedquantity        tiq
            ON tiq.RecID_AL     = al.recid
          LEFT JOIN purchaseagreementline_factquantity              tq
            ON tq.RecID_AL      = al.recid
          WHERE ah.instancerelationtype IN( 6827);
),
purchaseagreementline_factline AS (
    SELECT ts.LegalEntityID                                       AS LegalEntityID
             , ts.AgreementStateID                                    AS AgreementStateID
             , ts.VendorAccount                                       AS VendorAccount
             , ts.ItemID                                              AS ItemID
             , ts.ProductWidth                                        AS ProductWidth
             , ts.ProductLength                                       AS ProductLength
             , ts.ProductColor                                        AS ProductColor
             , ts.ProductConfig                                       AS ProductConfig
             , ts.AgreementUnit                                       AS AgreementUnit
             , ts.EffectiveDate                                       AS EffectiveDate
             , ts.ExpirationDate                                      AS ExpirationDate
             , ts.CreatedDate                                         AS CreatedDate
             , ts.BaseAmount_TransCur * ISNULL(ex.ExchangeRate, 1)    AS BaseAmount
             , ts.BaseAmount_TransCur                                 AS BaseAmount_TransCur
             , ts.BaseUnitPrice_TransCur * ISNULL(ex.ExchangeRate, 1) AS BaseUnitPrice
             , ts.BaseUnitPrice_TransCur                              AS BaseUnitPrice_TransCur
             , ts.TotalAmount_TransCur * ISNULL(ex.ExchangeRate, 1)   AS TotalAmount
             , ts.TotalAmount_TransCur                                AS TotalAmount_TransCur
             , ts.AgreementQuantity                                   AS AgreementQuantity
             , ts.InvoicedQuantity                                    AS InvoicedQuantity
             , ts.ReceivedQuantity                                    AS ReceivedQuantity
             , ts.ReleasedQuantity                                    AS ReleasedQuantity
             , ts.RemainingQuantity                                   AS RemainingQuantity
             , ts._RecID                                              AS _RecID
             , ts._SourceID                                           AS _SourceID

         FROM purchaseagreementline_factstage ts
         INNER JOIN {{ ref('date_d') }}              dd
            ON dd.Date             = ts.CreatedDate
         INNER JOIN {{ ref('legalentity_d') }}       le
            ON le.LegalEntityID    = ts.LegalEntityID
          LEFT JOIN {{ ref('exchangerate_f') }} ex
            ON ex.ExchangeDateKey  = dd.DateKey
           AND ex.FromCurrencyID   = ts.CurrencyID
           AND ex.ToCurrencyID     = le.AccountingCurrencyID
           AND ex.ExchangeRateType = le.TransExchangeRateType;
),
purchaseagreementline_factdetail1 AS (
    SELECT DISTINCT
               dsal.PurchaseAgreementLineKey AS PurchaseAgreementLineKey
             , das.AgreementStateKey         AS AgreementStateKey
             , du.UOMKey                     AS AgreementUOMKey
             , dc.VendorKey                  AS VendorKey
             , ISNULL(dp.ProductKey, -1)     AS ProductKey
             , dd.DateKey                    AS EffectiveDateKey
             , dd1.DateKey                   AS ExpirationDateKey
             , le.LegalEntityKey             AS LegalEntityKey
             , tl.AgreementQuantity          AS AgreementQuantity
             , tl.BaseAmount                 AS BaseAmount
             , tl.BaseAmount_TransCur        AS BaseAmount_TransCur
             , tl.BaseUnitPrice              AS BaseUnitPrice
             , tl.BaseUnitPrice_TransCur     AS BaseUnitPrice_TransCur
             , tl.InvoicedQuantity           AS InvoicedQuantity
             , tl.ReceivedQuantity           AS ReceivedQuantity
             , tl.ReleasedQuantity           AS ReleasedQuantity
             , tl.RemainingQuantity          AS RemainingQuantity
             , tl.TotalAmount                AS TotalAmount
             , tl.TotalAmount_TransCur       AS TotalAmount_TransCur
             , tl._RecID                     AS _RecID
             , tl._SourceID                  AS _SourceID

          FROM purchaseagreementline_factline                          tl
         INNER JOIN {{ ref('purchaseagreementline_d') }} dsal
            ON dsal._RecID          = tl._RecID
           AND dsal._SourceID       = 1
         INNER JOIN {{ ref('legalentity_d') }}           le
            ON le.LegalEntityID     = tl.LegalEntityID
          LEFT JOIN {{ ref('agreementstate_d') }}        das
           ON das.AgreementStateID = tl.AgreementStateID
          LEFT JOIN {{ ref('vendor_d') }}                dc
            ON dc.LegalEntityID     = tl.LegalEntityID
           AND dc.VendorAccount     = tl.VendorAccount
          LEFT JOIN {{ ref('product_d') }}               dp
            ON dp.LegalEntityID     = tl.LegalEntityID
           AND dp.ItemID            = tl.ItemID
           AND dp.ProductWidth      = tl.ProductWidth
           AND dp.ProductLength     = tl.ProductLength
           AND dp.ProductColor      = tl.ProductColor
           AND dp.ProductConfig     = tl.ProductConfig
          LEFT JOIN {{ ref('date_d') }}                  dd
            ON dd.Date              = tl.EffectiveDate
          LEFT JOIN {{ ref('date_d') }}                  dd1
            ON dd1.Date             = tl.ExpirationDate
          LEFT JOIN {{ ref('uom_d') }}                   du
            ON du.UOM               = tl.AgreementUnit;
)
SELECT 
           CURRENT_TIMESTAMP                                                                      AS _CreatedDate
         , CURRENT_TIMESTAMP                                                                      AS _ModifiedDate 
         , tl.PurchaseAgreementLineKey							   AS PurchaseAgreementLineKey
         , tl.AgreementStateKey									   AS AgreementStateKey
         , tl.AgreementUOMKey									   AS AgreementUOMKey
         , tl.VendorKey											   AS VendorKey
         , tl.ProductKey										   AS ProductKey
         , tl.EffectiveDateKey									   AS EffectiveDateKey
         , tl.ExpirationDateKey									   AS ExpirationDateKey
         , tl.LegalEntityKey									   AS LegalEntityKey
         , tl.AgreementQuantity									   AS AgreementQuantity
         , tl.AgreementQuantity * ISNULL(vuc.factor, 0)            AS AgreementQuantity_FT

         , tl.AgreementQuantity * ISNULL(vuc2.factor, 0)           AS AgreementQuantity_LB
         , ROUND(tl.AgreementQuantity * ISNULL(vuc3.factor, 0), 0) AS AgreementQuantity_PC
         , tl.AgreementQuantity * ISNULL(vuc4.factor, 0)           AS AgreementQuantity_SQIN

         , tl.BaseAmount										   AS BaseAmount
         , tl.BaseAmount_TransCur								   AS BaseAmount_TransCur
         , tl.BaseUnitPrice										   AS BaseUnitPrice
         , tl.BaseUnitPrice_TransCur							   AS BaseUnitPrice_TransCur
         , tl.InvoicedQuantity									   AS InvoicedQuantity
         , tl.InvoicedQuantity * ISNULL(vuc.factor, 0)             AS InvoicedQuantity_FT

         , tl.InvoicedQuantity * ISNULL(vuc2.factor, 0)            AS InvoicedQuantity_LB
         , ROUND(tl.InvoicedQuantity * ISNULL(vuc3.factor, 0), 0)  AS InvoicedQuantity_PC
         , tl.InvoicedQuantity * ISNULL(vuc4.factor, 0)            AS InvoicedQuantity_SQIN

         , tl.ReceivedQuantity									   AS ReceivedQuantity
         , tl.ReceivedQuantity * ISNULL(vuc.factor, 0)             AS ReceivedQuantity_FT

         , tl.ReceivedQuantity * ISNULL(vuc2.factor, 0)            AS ReceivedQuantity_LB
         , ROUND(tl.ReceivedQuantity * ISNULL(vuc3.factor, 0), 0)  AS ReceivedQuantity_PC
         , tl.ReceivedQuantity * ISNULL(vuc4.factor, 0)            AS ReceivedQuantity_SQIN

         , tl.ReleasedQuantity									   AS ReleasedQuantity
         , tl.ReleasedQuantity * ISNULL(vuc.factor, 0)             AS ReleasedQuantity_FT

         , tl.ReleasedQuantity * ISNULL(vuc2.factor, 0)            AS ReleasedQuantity_LB
         , ROUND(tl.ReleasedQuantity * ISNULL(vuc3.factor, 0), 0)  AS ReleasedQuantity_PC
         , tl.ReleasedQuantity * ISNULL(vuc4.factor, 0)            AS ReleasedQuantity_SQIN

         , tl.RemainingQuantity									   AS RemainingQuantity
         , tl.RemainingQuantity * ISNULL(vuc.factor, 0)            AS RemainingQuantity_FT

         , tl.RemainingQuantity * ISNULL(vuc2.factor, 0)           AS RemainingQuantity_LB
         , ROUND(tl.RemainingQuantity * ISNULL(vuc3.factor, 0), 0) AS RemainingQuantity_PC
         , tl.RemainingQuantity * ISNULL(vuc4.factor, 0)           AS RemainingQuantity_SQIN

         , tl.TotalAmount										   AS TotalAmount
         , tl.TotalAmount_TransCur								   AS TotalAmount_TransCur
         , tl._RecID											   AS _RecID
         , tl._SourceID											   AS _SourceID

      FROM purchaseagreementline_factdetail1              tl
      LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc
        ON vuc.legalentitykey  = tl.LegalEntityKey
       AND vuc.productkey      = tl.ProductKey
       AND vuc.fromuomkey      = tl.AgreementUOMKey
    -- AND vuc.touom           = 'FT'





      LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc2
        ON vuc2.legalentitykey = tl.LegalEntityKey
       AND vuc2.productkey     = tl.ProductKey
       AND vuc2.fromuomkey     = tl.AgreementUOMKey
    -- AND vuc2.touom          = 'LB'
      LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc3
        ON vuc3.legalentitykey = tl.LegalEntityKey
       AND vuc3.productkey     = tl.ProductKey
       AND vuc3.fromuomkey     = tl.AgreementUOMKey
    -- AND vuc3.touom          = 'PC'
      LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc4
        ON vuc4.legalentitykey = tl.LegalEntityKey
       AND vuc4.productkey     = tl.ProductKey
       AND vuc4.fromuomkey     = tl.AgreementUOMKey
    -- AND vuc4.touom          = 'SQIN'
