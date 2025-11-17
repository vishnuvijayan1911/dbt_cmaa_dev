{{ config(materialized='table', tags=['silver'], alias='customer_fact') }}

-- Source file: cma/cma/layers/_base/_silver/customer_f/customer_f.py
-- Root method: CustomerFact.get_detail_query [Customer_FactDetail]
-- Inlined methods: CustomerFact.get_address_query [Customer_FactAdress], CustomerFact.get_amount_mst_query [Customer_FactAmountMST], CustomerFact.get_stage_query [Customer_FactStage]
-- external_table_name: Customer_FactDetail
-- schema_name: temp

WITH
customer_factadress AS (
    SELECT Location
          , MAX(_RecID) AS RecID
     FROM {{ ref('address_d') }}
    WHERE LocationRank = 1
    GROUP BY Location;
),
customer_factamountmst AS (
    SELECT cto.dataareaid
          , cto.accountnum
          , SUM(cto.amountmst) AS AMOUNTMST
     FROM {{ ref('custtable') }}          ct
    INNER JOIN {{ ref('custtransopen') }} cto
       ON cto.dataareaid = ct.dataareaid
      AND cto.accountnum  = ct.accountnum
    GROUP BY cto.dataareaid
           , cto.accountnum;
),
customer_factstage AS (
    SELECT  ct.accountnum                                                                                              AS CustomerAccount
           , ct.dataareaid                                                                                              AS LegalEntityID
           , ct.currency                                                                                                AS CurrencyID
           , ct.custgroup                                                                                               AS CustomerGroupID
           , ct.inventlocation                                                                                          AS DefaultWarehouseID
           , ct.inventsiteid                                                                                            AS DefaultSiteID
           , ct.dlvterm                                                                                                 AS DeliveryTermID
           , ct.dlvmode                                                                                                 AS DeliveryModeID
           , ct.paymtermid                                                                                              AS PaymentTermID
           , ct.paymmode                                                                                                AS PaymentModeID
           , CASE WHEN ta.AMOUNTMST > 0 THEN 1 ELSE 0 END                                                               AS BalanceStatusID
           , CASE WHEN ct.creditmax < ta.AMOUNTMST THEN 1 WHEN ct.creditmax > ta.AMOUNTMST THEN 2 ELSE 0 END            AS CreditStatusID
           , ct.party                                                                                                   AS Party
           , ct.credmancustcreditmaxalt                                                                                 AS CreditLimit_CustomerCur
           , dpt.primaryaddresslocation                                                                                 AS Location
           , hcm.recid                                                                                                  AS RecID_HCM
           , ISNULL(ta.AMOUNTMST, 0)                                                                                    AS OpenBalance
           , ct.creditmax                                                                                               AS CreditLimit
           , CASE WHEN ISNULL(ta.AMOUNTMST, 0) <= 0
                    OR (ISNULL(ct.creditmax, 0) = 0)
                  THEN 0
                  ELSE CASE WHEN (ta.AMOUNTMST) - (ct.creditmax) < 0 THEN 0 ELSE (ta.AMOUNTMST) - (ct.creditmax) END END AS OverCreditLimit
           , CAST(ct.createddatetime  AS DATE)                                                                           AS CreatedDate 
           , ct.modifieddatetime                                                                                         AS _SourceDate
           , ct.recid                                                                                                    AS _RecID
           , 1                                                                                                           AS _SourceID
     FROM  {{ ref('custtable') }}          ct
     LEFT JOIN {{ ref('hcmworker') }}     hcm
       ON hcm.recid     = ct.maincontactworker
    INNER JOIN  {{ ref('dirpartytable') }} dpt
       ON dpt.recid     = ct.party
     LEFT JOIN customer_factamountmst      ta
       ON ta.DATAAREAID = ct.dataareaid
      AND ta.ACCOUNTNUM  = ct.accountnum;
)
SELECT  dv.CustomerKey                                                                                                  AS CustomerKey
       , le.LegalEntityKey                                                                                               AS LegalEntityKey
       , da.AddressKey                                                                                                   AS AddressKey
       , bal.BalanceStatusKey                                                                                            AS BalanceStatusKey
       , dc.CurrencyKey                                                                                                  AS CurrencyKey
       , cg.CustomerGroupKey                                                                                             AS CustomerGroupKey
       , ds.InventorySiteKey                                                                                             AS DefaultInventorySiteKey
       , dw.WarehouseKey                                                                                                 AS DefaultWarehouseKey
       , ddm.DeliveryModeKey                                                                                             AS DeliveryModeKey
       , ddt.DeliveryTermKey                                                                                             AS DeliveryTermKey
       , dpt.paymenttermkey                                                                                              AS PaymentTermKey
       , dpm.PaymentModeKey                                                                                              AS PaymentModeKey
       , dsp.SalesPersonKey                                                                                              AS SalesPersonKey
       , ts.CreditLimit                                                                                                  AS CreditLimit
       , ts.CreditLimit * ISNULL (ex.ExchangeRate, 1)                                                                    AS CreditLimit_TransCur
       , ts.CreditLimit_CustomerCur                                                                                      AS CreditLimit_CustomerCur
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
       , CASE WHEN ISNULL (ts.OverCreditLimit * ISNULL (ex.ExchangeRate, 1), 0) <= 0
              THEN (ts.CreditLimit - ts.OpenBalance) * ISNULL (ex.ExchangeRate, 1)
              ELSE 0 END                                                                                                 AS RemainingCredit_TransCur
       , CASE WHEN ISNULL (ts.OverCreditLimit * ex1.ExchangeRate, 0) <= 0
              THEN (ts.CreditLimit - ts.OpenBalance) * ex1.ExchangeRate
              ELSE 0 END                                                                                                 AS RemainingCredit_CAD
       , CASE WHEN ISNULL (ts.OverCreditLimit * ex2.ExchangeRate, 0) <= 0
              THEN (ts.CreditLimit - ts.OpenBalance) * ex2.ExchangeRate
              ELSE 0 END                                                                                                 AS RemainingCredit_MXP
       , CASE WHEN ISNULL (ts.OverCreditLimit * ex3.ExchangeRate, 0) <= 0
              THEN (ts.CreditLimit - ts.OpenBalance) * ex3.ExchangeRate
              ELSE 0 END                                                                                                 AS RemainingCredit_USD
       , ts._SourceDate                                                                                                  AS _SourceDate
       , ts._RecID                                                                                                       AS _RecID
       , ts._SourceID                                                                                                    AS _SourceID
       , CURRENT_TIMESTAMP                                                                                               AS _CreatedDate
       , CURRENT_TIMESTAMP                                                                                               AS _ModifiedDate  
 FROM   customer_factstage                    ts
INNER JOIN {{ ref('legalentity_d') }}         le
   ON le.LegalEntityID    = ts.LegalEntityID
INNER JOIN {{ ref('customer_d') }}            dv
   ON dv.LegalEntityID    = ts.LegalEntityID
  AND dv.CustomerAccount  = ts.CustomerAccount
 LEFT JOIN {{ ref('customergroup_d') }}       cg
   ON cg.LegalEntityID    = ts.LegalEntityID
  AND cg.CustomerGroupID  = ts.CustomerGroupID
 LEFT JOIN {{ ref('salesperson_d') }}         dsp
   ON dsp._RecID          = ts.RecID_HCM
  AND dsp._SourceID       = 1
 LEFT JOIN customer_factadress lpa
   ON lpa.Location        = ts.Location
 LEFT JOIN {{ ref('address_d') }}             da
   ON da._RecID           = lpa.RecID
 LEFT JOIN {{ ref('balance_status_d') }}       bal
   ON bal.BalanceStatusID = ts.BalanceStatusID
  AND bal.CreditStatusID  = ts.CreditStatusID
 LEFT JOIN {{ ref('deliverymode_d') }}        ddm
   ON ddm.LegalEntityID   = ts.LegalEntityID
  AND ddm.DeliveryModeID  = ts.DeliveryModeID
 LEFT JOIN {{ ref('deliveryterm_d') }}        ddt
   ON ddt.LegalEntityID   = ts.LegalEntityID
  AND ddt.DeliveryTermID  = ts.DeliveryTermID
 LEFT JOIN {{ ref('paymentterm_d') }}         dpt
   ON dpt.legalentityid   = ts.LegalEntityID
  AND dpt.paymenttermid   = ts.PaymentTermID
 LEFT JOIN {{ ref('paymentmode_d') }}         dpm
   ON dpm.LegalEntityID   = ts.LegalEntityID
  AND dpm.PaymentModeID   = ts.PaymentModeID
 LEFT JOIN {{ ref('currency_d') }}            dc
   ON dc.CurrencyID       = ts.CurrencyID
 LEFT JOIN {{ ref('warehouse_d') }}           dw
   ON dw.LegalEntityID    = ts.LegalEntityID
  AND dw.WarehouseID      = ts.DefaultWarehouseID
 LEFT JOIN {{ ref('inventorysite_d') }}       ds
   ON ds.LegalEntityID    = ts.LegalEntityID
  AND ds.InventorySiteID  = ts.DefaultSiteID
  LEFT JOIN {{ ref('date_d') }}                dd
   ON dd.Date              = ts.CreatedDate
  LEFT JOIN {{ ref('exchangerate_f') }}   ex
   ON ex.ExchangeDateKey   = dd.DateKey
  AND ex.FromCurrencyID    = le.AccountingCurrencyID
  AND ex.ToCurrencyID      = ts.CurrencyID
  AND ex.ExchangeRateType  = le.TransExchangeRateType
 LEFT JOIN {{ ref('exchangerate_f') }}   ex1
   ON ex1.ExchangeDateKey  = dd.DateKey
  AND ex1.FromCurrencyID   = le.AccountingCurrencyID
  AND ex1.ToCurrencyID     = 'CAD'
  AND ex1.ExchangeRateType = le.TransExchangeRateType
 LEFT JOIN {{ ref('exchangerate_f') }}   ex2
   ON ex2.ExchangeDateKey  = dd.DateKey
  AND ex2.FromCurrencyID   = le.AccountingCurrencyID
  AND ex2.ToCurrencyID     = 'MXN'
  AND ex2.ExchangeRateType = le.TransExchangeRateType
 LEFT JOIN {{ ref('exchangerate_f') }}   ex3
   ON ex3.ExchangeDateKey  = dd.DateKey
  AND ex3.FromCurrencyID   = le.AccountingCurrencyID
  AND ex3.ToCurrencyID     = 'USD'
  AND ex3.ExchangeRateType = le.TransExchangeRateType;
