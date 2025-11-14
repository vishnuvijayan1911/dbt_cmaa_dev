{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/salesinvoice_fact/salesinvoice_fact.py
-- Root method: SalesinvoiceFact.salesinvoice_factdetail [SalesInvoice_FactDetail]
-- Inlined methods: SalesinvoiceFact.salesinvoice_factlinetotal [SalesInvoice_FactLineTotal], SalesinvoiceFact.salesinvoice_factstage [SalesInvoice_FactStage]
-- external_table_name: SalesInvoice_FactDetail
-- schema_name: temp

WITH
salesinvoice_factlinetotal AS (
    SELECT cij.recid                                                          
             , SUM(cit.salesprice * cit.qty / ISNULL(NULLIF(cit.priceunit, 0), 1)) AS SumBaseAmount

          FROM {{ ref('custinvoicejour') }}       cij
         INNER JOIN {{ ref('custinvoicetrans') }} cit
            ON cit.dataareaid         = cij.dataareaid
           AND cit.salesid             = cij.salesid
           AND cit.invoiceid           = cij.invoiceid
           AND cit.invoicedate         = cij.invoicedate
           AND cit.numbersequencegroup = cij.numbersequencegroup
           AND (cit.parentrecid        = cij.recid OR cij.salestype <> 0)
         GROUP BY cij.recid;
),
salesinvoice_factstage AS (
    SELECT DISTINCT
               sh.workersalesresponsible              AS WorkerSalesResponsible
             , cij.dataareaid                        AS LegalEntityID
             , cit.currencycode                       AS CurrencyID
             , sh.dlvmode                             AS DeliveryModeID
             , sh.dlvterm                             AS DeliveryTermID
             , sh.payment                             AS PaymentTermID
             , sh.paymmode                            AS PaymentModeID
             , sh.taxgroup                            AS TaxGroupID
             , cij.ledgervoucher                      AS VoucherID
             , cij.invoiceaccount                     AS CustomerAccount
             , cij.invoiceaccount                     AS InvoiceAccount
             , cij.invoicedate                        AS InvoiceDate
             , cij.cashdisccode                       AS CashDiscountID
             , cij.salestype                          AS SalesTypeID
             , cij.refnum                             AS ReferenceTypeID
             , sh.recid                              AS RecID_ST
             , tl.SumBaseAmount * cij.exchrate / 100  AS SumBaseAmount
             , cij.enddisc * cij.exchrate / 100       AS SumDiscountAmount
             , cij.salesbalance * cij.exchrate / 100  AS SumNetAmount
             , cij.sumtax * cij.exchrate / 100        AS TaxAmount
             , cij.invoiceamount * cij.exchrate / 100 AS SumTotalAmount
             , cij.salesbalance * cij.exchrate / 100  AS SalesBalance
             , cij.summarkup * cij.exchrate / 100     AS SumCharges
             , cij.sumlinedisc * cij.exchrate / 100   AS SumLineDiscount
             , cij.sumtax * cij.exchrate / 100        AS SumTaxAmount
             , sh.salesgroup                          AS InsideSalesPerson
             , sh.workersalesresponsible              AS OutsideSalesPerson
             , sh.workersalestaker                    AS SalesTaker
             , cij.recid                              AS _RecID
             , 1                                      AS _SourceID

          FROM {{ ref('custinvoicejour') }}       cij
         INNER JOIN {{ ref('custinvoicetrans') }} cit
            ON cit.dataareaid         = cij.dataareaid
           AND cit.salesid             = cij.salesid
           AND cit.invoiceid           = cij.invoiceid
           AND cit.invoicedate         = cij.invoicedate
           AND cit.numbersequencegroup = cij.numbersequencegroup
           AND (cit.parentrecid        = cij.recid OR cij.salestype <> 0)
          LEFT JOIN  {{ ref('salestable') }}        sh
            ON sh.dataareaid          = cij.dataareaid
           AND sh.salesid              = cij.salesid
          LEFT JOIN {{ ref('salesline') }}        sl
            ON sl.dataareaid          = cit.dataareaid
           AND sl.inventtransid        = cit.inventtransid
           AND sl.itemid               = cit.itemid
         INNER JOIN salesinvoice_factlinetotal           tl
            ON tl.RECID               = cij.recid
          LEFT JOIN {{ ref('custtrans') }}        ct
            ON ct.invoice              = cij.invoiceid
           AND ct.accountnum           = cij.invoiceaccount
           AND ct.transdate            = cij.invoicedate
           AND ct.voucher              = cij.ledgervoucher;
)
SELECT DISTINCT
           CURRENT_TIMESTAMP                                                                      AS _CreatedDate
         , CURRENT_TIMESTAMP                                                                      AS _ModifiedDate 
         , dsi.SalesInvoiceKey    AS SalesInvoiceKey
         , cd.CashDiscountKey     AS CashDiscountKey
         , dc.CustomerKey         AS CustomerKey
         , cc.CurrencyKey         AS CurrencyKey
         , dt.DeliveryTermKey     AS DeliveryTermKey
         , dm.DeliveryModeKey     AS DeliveryModeKey
         , dsg.SalesGroupKey      AS InsideSalesPersonKey
         , dc2.CustomerKey        AS InvoiceCustomerKey
         , dd.DateKey             AS InvoiceDateKey
         , le.LegalEntityKey      AS LegalEntityKey
         , de1.EmployeeKey        AS OutsideSalesPersonKey
         , pyt.PaymentTermKey     AS PaymentTermKey
         , ivt.InvoiceTypeKey            AS InvoiceTypeKey
         , pm.PaymentModeKey                                  AS PaymentModeKey
         , dsil.SalesOrderLineKey AS SalesOrderLineKey
         , dsp.SalesPersonKey     AS SalesPersonKey
         , de2.EmployeeKey        AS SalesTakerKey
         , sit.SalesTypeKey       AS SalesTypeKey
         , tg.TaxGroupKey         AS TaxGroupKey
         , vou.VoucherKey         AS VoucherKey
         , ts.SumBaseAmount       AS SumBaseAmount
         , ts.SumCharges          AS SumCharges
         , ts.SumDiscountAmount   AS SumDiscountAmount
         , ts.SumLineDiscount     AS SumLineDiscount
         , ts.SumNetAmount        AS SumNetAmount
         , ts.SumTotalAmount      AS SumTotalAmount
         , ts.SumTaxAmount        AS SumTaxAmount
         , ts.SalesBalance        AS SalesBalance
         , ts.TaxAmount           AS TaxAmount
         , ts._RecID              AS _RecID
         , ts._SourceID           AS _SourceID

      FROM salesinvoice_factstage                  ts
     INNER JOIN silver.cma_LegalEntity    le
        ON le.LegalEntityID    = ts.LegalEntityID
     INNER JOIN silver.cma_SalesInvoice   dsi
        ON dsi._RecID          = ts._RecID
       AND dsi._SourceID       = 1
      LEFT JOIN silver.cma_SalesPerson    dsp
        ON dsp._RecID          = ts.WorkerSalesResponsible
       AND dsp._SourceID       = 1
      LEFT JOIN silver.cma_Customer       dc
        ON dc.LegalEntityID    = ts.LegalEntityID
       AND dc.CustomerAccount  = ts.CustomerAccount
      LEFT JOIN silver.cma_Customer       dc2
        ON dc2.LegalEntityID   = ts.LegalEntityID
       AND dc2.CustomerAccount = ts.InvoiceAccount
      LEFT JOIN silver.cma_Date           dd
        ON dd.Date             = ts.InvoiceDate
      LEFT JOIN silver.cma_InvoiceType    ivt
        ON ivt.InvoiceTypeID   = ts.ReferenceTypeID
      LEFT JOIN silver.cma_SalesOrderLine dsil
        ON dsil._RecID         = ts.RecID_ST
       AND dsil._SourceID      = 1
      LEFT JOIN silver.cma_Voucher        vou
        ON vou.LegalEntityID   = ts.LegalEntityID
       AND vou.VoucherID       = ts.VoucherID
      LEFT JOIN silver.cma_Currency       cc
        ON cc.CurrencyID       = ts.CurrencyID
      LEFT JOIN silver.cma_DeliveryMode   dm
        ON dm.LegalEntityID    = ts.LegalEntityID
       AND dm.DeliveryModeID   = ts.DeliveryModeID
      LEFT JOIN silver.cma_DeliveryTerm   dt
        ON dt.LegalEntityID    = ts.LegalEntityID
       AND dt.DeliveryTermID   = ts.DeliveryTermID
      LEFT JOIN silver.cma_PaymentTerm    pyt
        ON pyt.LegalEntityID   = ts.LegalEntityID
       AND pyt.PaymentTermID   = ts.PaymentTermID
      LEFT JOIN silver.cma_PaymentMode       pm
        ON pm.LegalEntityID     = ts.LegalEntityID
       AND pm.PaymentModeID     = ts.PaymentModeID
      LEFT JOIN silver.cma_TaxGroup       tg
        ON tg.LegalEntityID    = ts.LegalEntityID
       AND tg.TaxGroupID       = ts.TaxGroupID
      LEFT JOIN silver.cma_CashDiscount   cd
        ON cd.LegalEntityID    = ts.LegalEntityID
       AND cd.CashDiscountID   = ts.CashDiscountID
      LEFT JOIN silver.cma_SalesType      sit
        ON sit.SalesTypeID     = ts.SalesTypeID
      LEFT JOIN silver.cma_SalesGroup     dsg
        ON dsg.SalesGroupID    = ts.InsideSalesPerson
      LEFT JOIN silver.cma_Employee       de1
        ON de1._RecID          = ts.OutsideSalesPerson
      LEFT JOIN silver.cma_Employee       de2
        ON de2._RecID          = ts.SalesTaker;
