{{ config(materialized='table', tags=['silver'], alias='vendor_fact') }}

-- Source file: cma/cma/layers/_base/_silver/vendor_f/vendor_f.py
-- Root method: VendorFact.get_detail_query [Vendor_FactDetail]
-- Inlined methods: VendorFact.get_address_query [Vendor_FactLogisticsPostalAddress], VendorFact.get_amount_mst_query [Vendor_FactOpenAmount], VendorFact.get_stage_query [Vendor_FactStage]
-- external_table_name: Vendor_FactDetail
-- schema_name: temp

WITH
vendor_factlogisticspostaladdress AS (
    SELECT Location    AS Location
         , MAX(_RecID) AS RecID
    FROM {{ ref('d365cma_address_d') }}
    WHERE LocationRank = 1
    GROUP BY Location;
),
vendor_factopenamount AS (
    SELECT vto.dataareaid
          , vto.accountnum
          , SUM(vto.amountmst * -1) AS AMOUNTMST
     FROM {{ ref('vendtable') }}     vt
     JOIN {{ ref('vendtransopen') }} vto
       ON vto.dataareaid = vt.dataareaid
      AND vto.accountnum  = vt.accountnum
    GROUP BY vto.dataareaid
           , vto.accountnum;
),
vendor_factstage AS (
    SELECT vt.accountnum                                                                                               AS VendorAccount
          , vt.dataareaid                                                                                               AS LegalEntityID
          , vt.currency                                                                                                 AS CurrencyID
          , vt.inventlocation                                                                                           AS DefaultWarehouseID
          , vt.inventsiteid                                                                                             AS DefaultSiteID
          , vt.dlvterm                                                                                                  AS DeliveryTermID
          , vt.dlvmode                                                                                                  AS DeliveryModeID
          , vt.party                                                                                                    AS Party
          , vt.paymtermid                                                                                               AS PaymentTermID
          , vt.paymmode                                                                                                 AS PaymentModeID
          , CASE WHEN ta.AMOUNTMST > 0 THEN 1 ELSE 0 END                                                                AS BalanceStatusID
          , CASE WHEN vt.creditmax < ta.AMOUNTMST THEN 1 WHEN vt.creditmax > ta.AMOUNTMST THEN 2 ELSE 0 END             AS CreditStatusID
          , dpt.primaryaddresslocation                                                                                  AS Location
          , hcm.recid                                                                                                   AS RecID_HCM
          , ISNULL(ta.AMOUNTMST, 0)                                                                                     AS OpenBalance
          , vt.creditmax                                                                                                AS CreditLimit
          , CASE WHEN ISNULL(ta.AMOUNTMST, 0) <= 0
                   OR (ISNULL(vt.creditmax, 0) = 0)
                 THEN 0
                 ELSE CASE WHEN (ta.AMOUNTMST) - (vt.creditmax) < 0 THEN 0 ELSE (ta.AMOUNTMST) - (vt.creditmax) END END AS OverCreditLimit
          , CAST(vt.createddatetime AS DATE)                                                                            AS CreatedDate
          , vt.modifieddatetime                                                                                         AS _SourceDate
          , vt.recid                                                                                                    AS _RecID
          , 1                                                                                                           AS _SourceID
     FROM {{ ref('vendtable') }}          vt
     LEFT JOIN {{ ref('hcmworker') }}     hcm
       ON hcm.recid     = vt.maincontactworker
    INNER JOIN  {{ ref('dirpartytable') }} dpt
       ON dpt.recid     = vt.party
     LEFT JOIN vendor_factopenamount       ta
       ON ta.DATAAREAID = vt.dataareaid
      AND ta.ACCOUNTNUM  = vt.accountnum;
)
SELECT dv.VendorKey                                                                                                    AS VendorKey
       , da.AddressKey                                                                                                   AS AddressKey
       , bal.BalanceStatusKey                                                                                            AS BalanceStatusKey
       , cur.CurrencyKey                                                                                                 AS CurrencyKey
       , dd.DateKey                                                                                                      AS CreatedDateKey
       , dis.InventorySiteKey                                                                                            AS DefaultInventorySiteKey
       , dw.WarehouseKey                                                                                                 AS DefaultWarehouseKey
       , dm.DeliveryModeKey                                                                                              AS DeliveryModeKey
       , tm.DeliveryTermKey                                                                                              AS DeliveryTermKey
       , le.LegalEntityKey                                                                                               AS LegalEntityKey
       , pm.PaymentModeKey                                                                                               AS PaymentModeKey
       , pa.PaymentTermKey                                                                                               AS PaymentTermKey
       , ts.CreditLimit                                                                                                  AS CreditLimit
       , ts.CreditLimit * ISNULL (ex.ExchangeRate, 1)                                                                    AS CreditLimit_TransCur
       , ts.CreditLimit * ex1.ExchangeRate                                                                               AS CreditLimit_CAD
       , ts.CreditLimit * ex2.ExchangeRate                                                                               AS CreditLimit_MXP
       , ts.CreditLimit * ex3.ExchangeRate                                                                               AS CreditLimit_USD
       , ts.OpenBalance                                                                                                  AS OpenBalance
       , ts.OpenBalance * ISNULL (ex.ExchangeRate, 1)                                                                    AS OpenBalance_TransCur
       , ts.OpenBalance * ex1.ExchangeRate                                                                               AS OpenBalance_CAD
       , ts.OpenBalance * ex2.ExchangeRate                                                                               AS OpenBalance_MXP
       , ts.OpenBalance * ex3.ExchangeRate                                                                               AS OpenBalance_USD
       , ts.OverCreditLimit                                                                                              AS OverCreditLimit
       , ts.OverCreditLimit * ISNULL (ex.ExchangeRate, 1)                                                                AS OverCreditLimit_TransCur
       , ts.OverCreditLimit * ex1.ExchangeRate                                                                           AS OverCreditLimit_CAD
       , ts.OverCreditLimit * ex2.ExchangeRate                                                                           AS OverCreditLimit_MXP
       , ts.OverCreditLimit * ex3.ExchangeRate                                                                           AS OverCreditLimit_USD
       , CASE WHEN ts.CreditLimit - ts.OpenBalance > 0 AND ts.CreditLimit > 0 THEN ts.CreditLimit - ts.OpenBalance ELSE
                                                                                                                   0 END AS RemainingCredit
   ,     CASE WHEN (ts.CreditLimit - ts.OpenBalance) * ISNULL (ex.ExchangeRate, 1) > 0
               AND ts.CreditLimit * ISNULL (ex.ExchangeRate, 1) > 0
              THEN (ts.CreditLimit - ts.OpenBalance) * ISNULL (ex.ExchangeRate, 1)
              ELSE 0 END                                                                                                 AS RemainingCredit_TransCur
       , CASE WHEN (ts.CreditLimit - ts.OpenBalance) * ex1.ExchangeRate > 0
               AND ts.CreditLimit * ex1.ExchangeRate > 0
              THEN (ts.CreditLimit - ts.OpenBalance) * ex1.ExchangeRate
              ELSE 0 END                                                                                                 AS RemainingCredit_CAD
       , CASE WHEN (ts.CreditLimit - ts.OpenBalance) * ex2.ExchangeRate > 0
               AND ts.CreditLimit * ex2.ExchangeRate > 0
              THEN (ts.CreditLimit - ts.OpenBalance) * ex2.ExchangeRate
              ELSE 0 END                                                                                                 AS RemainingCredit_MXP
       , CASE WHEN (ts.CreditLimit - ts.OpenBalance) * ex3.ExchangeRate > 0
               AND ts.CreditLimit * ex3.ExchangeRate > 0
              THEN (ts.CreditLimit - ts.OpenBalance) * ex3.ExchangeRate
              ELSE 0 END                                                                                                 AS RemainingCredit_USD
       , ts._RecID                                                                                                       AS _RecID
       , ts._SourceID                                                                                                    AS _SourceID
 FROM vendor_factstage                       ts
INNER JOIN {{ ref('d365cma_legalentity_d') }}         le
   ON le.LegalEntityID    = ts.LegalEntityID
INNER JOIN {{ ref('d365cma_vendor_d') }}              dv
   ON dv.LegalEntityID    = ts.LegalEntityID
  AND dv.VendorAccount    = ts.VendorAccount
 LEFT JOIN vendor_factlogisticspostaladdress lpa
   ON lpa.Location        = ts.Location
 LEFT JOIN {{ ref('d365cma_address_d') }}             da
   ON da._RecID           = lpa.RecID
 LEFT JOIN {{ ref('d365cma_date_d') }}                dd
   ON dd.Date             = ts.CreatedDate
 LEFT JOIN {{ ref('d365cma_balance_status_d') }}       bal
   ON bal.BalanceStatusID = ts.BalanceStatusID
  AND bal.CreditStatusID  = ts.CreditStatusID
 LEFT JOIN {{ ref('d365cma_deliverymode_d') }}        dm
   ON dm.LegalEntityID    = ts.LegalEntityID
  AND dm.DeliveryModeID   = ts.DeliveryModeID
 LEFT JOIN {{ ref('d365cma_deliveryterm_d') }}        tm
   ON tm.LegalEntityID    = ts.LegalEntityID
  AND tm.DeliveryTermID   = ts.DeliveryTermID
 LEFT JOIN {{ ref('d365cma_paymentterm_d') }}         pa
   ON pa.LegalEntityID    = ts.LegalEntityID
  AND pa.PaymentTermID    = ts.PaymentTermID
 LEFT JOIN {{ ref('d365cma_paymentmode_d') }}         pm
   ON pm.LegalEntityID    = ts.LegalEntityID
  AND pm.PaymentModeID    = ts.PaymentModeID
 LEFT JOIN {{ ref('d365cma_currency_d') }}            cur
   ON cur.CurrencyID      = ts.CurrencyID
 LEFT JOIN {{ ref('d365cma_warehouse_d') }}           dw
   ON dw.LegalEntityID    = ts.LegalEntityID
  AND dw.WarehouseID      = ts.DefaultWarehouseID
 LEFT JOIN {{ ref('d365cma_inventorysite_d') }}       dis
   ON dis.LegalEntityID   = ts.LegalEntityID
  AND dis.InventorySiteID = ts.DefaultSiteID
  LEFT JOIN {{ ref('d365cma_exchangerate_f') }}   ex
   ON ex.ExchangeDateKey   = dd.DateKey
  AND ex.FromCurrencyID    = le.AccountingCurrencyID
  AND ex.ToCurrencyID      = ts.CurrencyID
  AND ex.ExchangeRateType  = le.TransExchangeRateType
 LEFT JOIN {{ ref('d365cma_exchangerate_f') }}   ex1
   ON ex1.ExchangeDateKey  = dd.DateKey
  AND ex1.FromCurrencyID   = le.AccountingCurrencyID
  AND ex1.ToCurrencyID     = 'CAD'
  AND ex1.ExchangeRateType = le.TransExchangeRateType
 LEFT JOIN {{ ref('d365cma_exchangerate_f') }}   ex2
   ON ex2.ExchangeDateKey  = dd.DateKey
  AND ex2.FromCurrencyID   = le.AccountingCurrencyID
  AND ex2.ToCurrencyID     = 'MXN'
  AND ex2.ExchangeRateType = le.TransExchangeRateType
 LEFT JOIN {{ ref('d365cma_exchangerate_f') }}   ex3
   ON ex3.ExchangeDateKey  = dd.DateKey
  AND ex3.FromCurrencyID   = le.AccountingCurrencyID
  AND ex3.ToCurrencyID     = 'USD'
  AND ex3.ExchangeRateType = le.TransExchangeRateType;
