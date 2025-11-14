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
  FROM {{ ref("SalesInvoice") }}           t 
INNER JOIN {{ ref("SalesInvoice_Fact") }} f 
    ON f.SalesInvoiceKey     = t.SalesInvoiceKey
INNER JOIN {{ ref("Currency") }}          c 
    ON c.CurrencyKey         = f.CurrencyKey
INNER JOIN {{ ref("SalesType") }}         st 
    ON st.SalesTypeKey       = f.SalesTypeKey
  LEFT JOIN {{ ref("Employee") }}          e3 
    ON e3.EmployeeKey        = f.SalesTakerKey
  LEFT JOIN {{ ref("DeliveryMode") }}      dm 
    ON dm.DeliveryModeKey    = f.DeliveryModeKey
  LEFT JOIN {{ ref("DeliveryTerm") }}      dt 
    ON dt.DeliveryTermKey    = f.DeliveryTermKey
  LEFT JOIN {{ ref("PaymentTerm") }}       pat 
    ON pat.PaymentTermKey    = f.PaymentTermKey
  LEFT JOIN {{ ref("TaxGroup") }}          tg 
    ON tg.TaxGroupKey        = f.TaxGroupKey
  LEFT JOIN {{ ref("Voucher") }}           vou 
    ON vou.VoucherKey        = f.VoucherKey
  LEFT JOIN {{ ref("InvoiceType") }}       it 
    ON it.InvoiceTypeKey     = f.InvoiceTypeKey
  LEFT JOIN {{ ref("SalesPerson") }}       sp 
    ON sp.SalesPersonKey     = f.SalesPersonKey
  LEFT JOIN {{ ref("PaymentMode") }}       pam 
    ON pam.PaymentModeKey    = f.PaymentModeKey;
