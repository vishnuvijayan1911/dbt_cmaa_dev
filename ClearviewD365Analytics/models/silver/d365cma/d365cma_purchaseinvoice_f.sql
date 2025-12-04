{{ config(materialized='table', tags=['silver'], alias='purchaseinvoice_fact') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseinvoice_f/purchaseinvoice_f.py
-- Root method: PurchaseInvoiceFact.get_detail_query [PurchaseInvoice_FactDetail]
-- Inlined methods: PurchaseInvoiceFact.get_discount_query [PurchaseInvoice_FactDiscount], PurchaseInvoiceFact.get_currency_query [PurchaseInvoice_FactCurrency], PurchaseInvoiceFact.get_stage_query [PurchaseInvoice_FactStage]
-- external_table_name: PurchaseInvoice_FactDetail
-- schema_name: temp

WITH
purchaseinvoice_factdiscount AS (
    SELECT vij.recid                             AS _RecID
          , SUM (ISNULL (vtcd.cashdiscamount, 0)) AS CashDiscAmount
     FROM {{ ref('vendinvoicejour') }}        vij
    INNER JOIN {{ ref('vendtrans') }}         vt
       ON vt.dataareaid    = vij.dataareaid
      AND vt.accountnum    = vij.invoiceaccount
      AND vt.transdate     = vij.invoicedate
      AND vt.voucher       = vij.ledgervoucher
    INNER JOIN {{ ref('vendsettlement') }}    vss
       ON vss.transcompany = vt.dataareaid
      AND vss.transrecid   = vt.recid
      AND vss.accountnum   = vt.accountnum
    INNER JOIN {{ ref('vendtranscashdisc') }} vtcd
       ON vtcd.refrecid    = vss.recid
    INNER JOIN {{ ref('sqldictionary') }}     sd
       ON sd.tabid       = vtcd.reftableid
      AND sd.name          = 'VendSettlement'
    GROUP BY vij.recid;
),
purchaseinvoice_factcurrency AS (
    SELECT vij.recid         AS _RecID
        , MAX (pt.dlvterm)  AS DeliveryTermID
        , MAX (pt.dlvmode)  AS DeliveryModeID
        , MAX (pt.payment)  AS PaymentTermID
        , MAX (pt.paymmode) AS PaymentModeID
        , MAX (pt.taxgroup) AS TaxGroupID
     FROM {{ ref('vendinvoicejour') }} vij
     LEFT JOIN {{ ref('purchtable') }} pt
       ON pt.dataareaid  = vij.dataareaid
      AND pt.purchid     = vij.purchid
    GROUP BY vij.recid;
),
purchaseinvoice_factstage AS (
    SELECT DISTINCT
         vij.dataareaid                         AS LegalEntityID
       , vij.ledgervoucher                      AS VoucherID
       , vij.currencycode                       AS CurrencyID
       , tc.DeliveryTermID                      AS DeliveryTermID
       , tc.DeliveryModeID                      AS DeliveryModeID
       , tc.PaymentModeID                       AS PaymentModeID
       , tc.PaymentTermID                       AS PaymentTermID
       , tc.TaxGroupID                          AS TaxGroupID
       , vij.invoiceaccount                     AS VendorAccount
       , vt.accountnum                          AS InvoiceAccount
       , vij.invoicedate                        AS InvoiceDate
       , vij.duedate                            AS DueDate
       , vij.cashdisccode                       AS CashDiscountID
       , vij.purchasetype                       AS PurchaseTypeID
       , vij.cashdisc * vij.exchrate / 100      AS CashDiscount
       , vij.cashdisc                           AS CashDiscount_TransCur
       , vij.cashdiscpercent                    AS CashDiscountPercent
       , td.CashDiscAmount * vij.exchrate / 100 AS DiscountLost
       , td.CashDiscAmount                      AS DiscountLost_TransCur
       , vij.salesbalance * vij.exchrate / 100  AS InvoiceNetAmount
       , vij.salesbalance                       AS InvoiceNetAmount_TransCur
       , vij.sumtax * vij.exchrate / 100        AS TaxAmount
       , vij.sumtax                             AS TaxAmount_TransCur
       , vij.invoiceamountmst                   AS InvoiceTotalAmount
       , vij.invoiceamount                      AS InvoiceTotalAmount_TransCur
       , vij.salesbalance * vij.exchrate / 100  AS PurchaseBalance
       , vij.salesbalance                       AS PurchaseBalance_TransCur
       , vij.summarkupmst                       AS SumCharges
       , vij.summarkup                          AS SumCharges_TransCur
       , vij.sumlinedisc * vij.exchrate / 100   AS SumLineDiscount
       , vij.sumlinedisc                        AS SumLineDiscount_TransCur
       , vij.sumtax * vij.exchrate / 100        AS SumTax
       , vij.sumtax                             AS SumTax_TransCur
       , vij.enddiscmst                         AS TotalDiscount
       , vij.enddisc                            AS TotalDiscount_TransCur
       , 1                                      AS _SourceID
       , vij.recid                              AS _RecID
    FROM {{ ref('vendinvoicejour') }} vij
    LEFT JOIN purchaseinvoice_factcurrency      tc
      ON tc._RecID      = vij.recid
    LEFT JOIN purchaseinvoice_factdiscount      td
      ON td._RecID      = vij.recid
    LEFT JOIN {{ ref('vendtrans') }}  vt
      ON vt.dataareaid  = vij.dataareaid
     AND vt.voucher     = vij.ledgervoucher
     AND vt.accountnum  = vij.invoiceaccount
     AND vt.transdate   = vij.invoicedate;
)
SELECT DISTINCT
      dsi.PurchaseInvoiceKey                   AS PurchaseInvoiceKey
    , cd.CashDiscountKey                       AS CashDiscountKey
    , cur.CurrencyKey                          AS CurrencyKey
    , tm.DeliveryTermKey                       AS DeliveryTermKey
    , dm.DeliveryModeKey                       AS DeliveryModeKey
    , pm.PaymentModeKey                        AS PaymentModeKey
    , dd1.DateKey                              AS DueDateKey
    , dd.DateKey                               AS InvoiceDateKey
    , dc2.VendorKey                            AS InvoiceVendorKey
    , le.LegalEntityKey                        AS LegalEntityKey
    , pa.PaymentTermKey                        AS PaymentTermKey
    , pit.PurchaseTypeKey                      AS PurchaseTypeKey
    , tg.TaxGroupKey                           AS TaxGroupKey
    , vou.VoucherKey                           AS VoucherKey
    , dc.VendorKey                             AS VendorKey
    , ts.CashDiscount                          AS CashDiscount
    , ts.CashDiscount * ex.ExchangeRate        AS CashDiscount_CAD
    , ts.CashDiscount * ex1.ExchangeRate       AS CashDiscount_MXP
    , ts.CashDiscount * ex2.ExchangeRate       AS CashDiscount_USD
    , ts.CashDiscount_TransCur                 AS CashDiscount_TransCur
    , ts.CashDiscountPercent                   AS CashDiscountPercent
    , ts.DiscountLost                          AS DiscountLost
    , ts.DiscountLost * ex.ExchangeRate        AS DiscountLost_CAD
    , ts.DiscountLost * ex1.ExchangeRate       AS DiscountLost_MXP
    , ts.DiscountLost * ex2.ExchangeRate       AS DiscountLost_USD
    , ts.DiscountLost_TransCur                 AS DiscountLost_TransCur
    , ts.InvoiceNetAmount                      AS InvoiceNetAmount
    , ts.InvoiceNetAmount * ex.ExchangeRate    AS InvoiceNetAmount_CAD
    , ts.InvoiceNetAmount * ex1.ExchangeRate   AS InvoiceNetAmount_MXP
    , ts.InvoiceNetAmount * ex2.ExchangeRate   AS InvoiceNetAmount_USD
    , ts.InvoiceNetAmount_TransCur             AS InvoiceNetAmount_TransCur
    , ts.InvoiceTotalAmount                    AS InvoiceTotalAmount
    , ts.InvoiceTotalAmount * ex.ExchangeRate  AS InvoiceTotalAmount_CAD
    , ts.InvoiceTotalAmount * ex1.ExchangeRate AS InvoiceTotalAmount_MXP
    , ts.InvoiceTotalAmount * ex2.ExchangeRate AS InvoiceTotalAmount_USD
    , ts.InvoiceTotalAmount_TransCur           AS InvoiceTotalAmount_TransCur
    , ts.PurchaseBalance                       AS PurchaseBalance
    , ts.PurchaseBalance * ex.ExchangeRate     AS PurchaseBalance_CAD
    , ts.PurchaseBalance * ex1.ExchangeRate    AS PurchaseBalance_MXP
    , ts.PurchaseBalance * ex2.ExchangeRate    AS PurchaseBalance_USD
    , ts.PurchaseBalance_TransCur              AS PurchaseBalance_TransCur
    , ts.SumCharges                            AS SumCharges
    , ts.SumCharges * ex.ExchangeRate          AS SumCharges_CAD
    , ts.SumCharges * ex1.ExchangeRate         AS SumCharges_MXP
    , ts.SumCharges * ex2.ExchangeRate         AS SumCharges_USD
    , ts.SumCharges_TransCur                   AS SumCharges_TransCur
    , ts.SumLineDiscount                       AS SumLineDiscount
    , ts.SumLineDiscount * ex.ExchangeRate     AS SumLineDiscount_CAD
    , ts.SumLineDiscount * ex1.ExchangeRate    AS SumLineDiscount_MXP
    , ts.SumLineDiscount * ex2.ExchangeRate    AS SumLineDiscount_USD
    , ts.SumLineDiscount_TransCur              AS SumLineDiscount_TransCur
    , ts.SumTax                                AS SumTax
    , ts.SumTax * ex.ExchangeRate              AS SumTax_CAD
    , ts.SumTax * ex1.ExchangeRate             AS SumTax_MXP
    , ts.SumTax * ex2.ExchangeRate             AS SumTax_USD
    , ts.SumTax_TransCur                       AS SumTax_TransCur
    , ts.TaxAmount                             AS TaxAmount
    , ts.TaxAmount * ex.ExchangeRate           AS TaxAmount_CAD
    , ts.TaxAmount * ex1.ExchangeRate          AS TaxAmount_MXP
    , ts.TaxAmount * ex2.ExchangeRate          AS TaxAmount_USD
    , ts.TaxAmount_TransCur                    AS TaxAmount_TransCur
    , ts.TotalDiscount                         AS TotalDiscount
    , ts.TotalDiscount * ex.ExchangeRate       AS TotalDiscount_CAD
    , ts.TotalDiscount * ex1.ExchangeRate      AS TotalDiscount_MXP
    , ts.TotalDiscount * ex2.ExchangeRate      AS TotalDiscount_USD
    , ts.TotalDiscount_TransCur                AS TotalDiscount_TransCur
    , ts._SourceID                             AS _SourceID
    , ts._RecID                                AS _RecID
 FROM purchaseinvoice_factstage    ts
INNER JOIN {{ ref('d365cma_legalentity_d') }}       le
   ON le.LegalEntityID     = ts.LegalEntityID
INNER JOIN {{ ref('d365cma_purchaseinvoice_d') }}   dsi
   ON dsi._RecID           = ts._RecID
  AND dsi._SourceID        = 1
 LEFT JOIN {{ ref('d365cma_currency_d') }}          cur
   ON cur.CurrencyID       = ts.CurrencyID
 LEFT JOIN {{ ref('d365cma_vendor_d') }}            dc
   ON dc.LegalEntityID     = ts.LegalEntityID
  AND dc.VendorAccount     = ts.VendorAccount
 LEFT JOIN {{ ref('d365cma_vendor_d') }}            dc2
   ON dc2.LegalEntityID    = ts.LegalEntityID
  AND dc2.VendorAccount    = ts.InvoiceAccount
 LEFT JOIN {{ ref('d365cma_date_d') }}              dd
   ON dd.Date              = ts.InvoiceDate
 LEFT JOIN {{ ref('d365cma_voucher_d') }}           vou
   ON vou.LegalEntityID    = ts.LegalEntityID
  AND vou.VoucherID        = ts.VoucherID
 LEFT JOIN {{ ref('d365cma_deliverymode_d') }}      dm
   ON dm.LegalEntityID     = ts.LegalEntityID
  AND dm.DeliveryModeID    = ts.DeliveryModeID
 LEFT JOIN {{ ref('d365cma_deliveryterm_d') }}      tm
   ON tm.LegalEntityID     = ts.LegalEntityID
  AND tm.DeliveryTermID    = ts.DeliveryTermID
 LEFT JOIN {{ ref('d365cma_paymentterm_d') }}       pa
   ON pa.LegalEntityID     = ts.LegalEntityID
  AND pa.PaymentTermID     = ts.PaymentTermID
 LEFT JOIN {{ ref('d365cma_taxgroup_d') }}          tg
   ON tg.LegalEntityID     = ts.LegalEntityID
  AND tg.TaxGroupID        = ts.TaxGroupID
 LEFT JOIN {{ ref('d365cma_cashdiscount_d') }}      cd
   ON cd.LegalEntityID     = ts.LegalEntityID
  AND cd.CashDiscountID    = ts.CashDiscountID
 LEFT JOIN {{ ref('d365cma_purchasetype_d') }}      pit
   ON pit.PurchaseTypeID   = ts.PurchaseTypeID
 LEFT JOIN {{ ref('d365cma_date_d') }}              dd1
   ON dd1.Date             = ts.DueDate
 LEFT JOIN {{ ref('d365cma_paymentmode_d') }}       pm
   ON pm.LegalEntityID     = ts.LegalEntityID
  AND pm.PaymentModeID     = ts.PaymentModeID
 LEFT JOIN {{ ref('d365cma_exchangerate_f') }} ex
   ON ex.ExchangeDateKey   = dd.DateKey
  AND ex.FromCurrencyID    = le.AccountingCurrencyID
  AND ex.ToCurrencyID      = 'CAD'
  AND ex.ExchangeRateType  = le.TransExchangeRateType
 LEFT JOIN {{ ref('d365cma_exchangerate_f') }} ex1
   ON ex1.ExchangeDateKey  = dd.DateKey
  AND ex1.FromCurrencyID   = le.AccountingCurrencyID
  AND ex1.ToCurrencyID     = 'MXN'
  AND ex1.ExchangeRateType = le.TransExchangeRateType
 LEFT JOIN {{ ref('d365cma_exchangerate_f') }} ex2
   ON ex2.ExchangeDateKey  = dd.DateKey
  AND ex2.FromCurrencyID   = le.AccountingCurrencyID
  AND ex2.ToCurrencyID     = 'USD'
  AND ex2.ExchangeRateType = le.TransExchangeRateType;
