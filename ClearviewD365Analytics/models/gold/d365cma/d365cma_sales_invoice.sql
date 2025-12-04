{{ config(materialized='view', schema='gold', alias="Sales invoice") }}

SELECT  t.SalesInvoiceKey                                                                   AS [Sales invoice key]
    , NULLIF(c.CurrencyID, '')                                                            AS [Currency]
    , NULLIF(dm.DeliveryModeID, '')                                                       AS [Delivery mode]
    , NULLIF(dm.DeliveryMode, '')                                                         AS [Delivery mode name]
    , NULLIF(dt.DeliveryTermID, '')                                                       AS [Delivery term]
    , NULLIF(dt.DeliveryTerm, '')                                                         AS [Delivery term name]
    , NULLIF(t.InvoiceID, '')                                                             AS [Invoice]
    , NULLIF(pam.PaymentModeID, '')                                                       AS [Payment mode]
    , NULLIF(pam.PaymentMode, '')                                                         AS [Payment mode name]
    , NULLIF(pat.PaymentTermID, '')                                                       AS [Payment term]
    , NULLIF(pat.PaymentTerm, '')                                                         AS [Payment term name]
    , NULLIF(t.SalesOrderID, '')                                                          AS [Sales order]
    , NULLIF(e3.EmployeeName, '')                                                         AS [Sales taker]
    , NULLIF(sp.SalesPerson, '')                                                          AS [Sales person]
    , NULLIF(st.SalesType, '')                                                            AS [Invoice sales type]
    , NULLIF(CASE WHEN it.InvoiceTypeID = 2 THEN 'Free text' ELSE it.InvoiceType END, '') AS [Invoice type]
    , NULLIF(tg.TaxGroup, '')                                                             AS [Tax group]
    , NULLIF(vou.VoucherID, '')                                                           AS [Voucher]
    , NULLIF(t.DueDate, '1/1/1900')                                                       AS [Due date]
    , NULLIF(t.InvoiceDate, '1/1/1900')                                                   AS [Invoice date]
  FROM {{ ref("d365cma_salesinvoice_d") }}           t 
INNER JOIN {{ ref("d365cma_salesinvoice_f") }} f 
    ON f.SalesInvoiceKey     = t.SalesInvoiceKey
INNER JOIN {{ ref("d365cma_currency_d") }}          c 
    ON c.CurrencyKey         = f.CurrencyKey
INNER JOIN {{ ref("d365cma_salestype_d") }}         st 
    ON st.SalesTypeKey       = f.SalesTypeKey
  LEFT JOIN {{ ref("d365cma_employee_d") }}          e3 
    ON e3.EmployeeKey        = f.SalesTakerKey
  LEFT JOIN {{ ref("d365cma_deliverymode_d") }}      dm 
    ON dm.DeliveryModeKey    = f.DeliveryModeKey
  LEFT JOIN {{ ref("d365cma_deliveryterm_d") }}      dt 
    ON dt.DeliveryTermKey    = f.DeliveryTermKey
  LEFT JOIN {{ ref("d365cma_paymentterm_d") }}       pat 
    ON pat.PaymentTermKey    = f.PaymentTermKey
  LEFT JOIN {{ ref("d365cma_taxgroup_d") }}          tg 
    ON tg.TaxGroupKey        = f.TaxGroupKey
  LEFT JOIN {{ ref("d365cma_voucher_d") }}           vou 
    ON vou.VoucherKey        = f.VoucherKey
  LEFT JOIN {{ ref("d365cma_invoicetype_d") }}       it 
    ON it.InvoiceTypeKey     = f.InvoiceTypeKey
  LEFT JOIN {{ ref("d365cma_salesperson_d") }}       sp 
    ON sp.SalesPersonKey     = f.SalesPersonKey
  LEFT JOIN {{ ref("d365cma_paymentmode_d") }}       pam 
    ON pam.PaymentModeKey    = f.PaymentModeKey;
