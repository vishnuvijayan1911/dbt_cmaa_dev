{{ config(materialized='view', schema='gold', alias="Sales invoice line fact") }}

WITH Charges
  AS (
    SELECT  SalesInvoiceLineKey
          , SUM(NonBillableCharge)          AS NonBillableCharge
          , SUM(NonBillableCharge_TransCur) AS NonBillableCharge_TransCur
      FROM {{ ref("SalesInvoiceLineCharge_Fact") }}
      GROUP BY SalesInvoiceLineKey)
SELECT  t.SalesInvoiceLineKey                                                                                               AS [Sales invoice line key]
    , t.SalesInvoiceKey                                                                                                   AS [Sales invoice key]
    , t.CustomerKey                                                                                                       AS [Customer key]
    , t.InvoiceCustomerKey                                                                                                AS [Invoice customer key]
    , t.InvoiceDateKey                                                                                                    AS [Invoice date key]
    , t.DueDateKey                                                                                                        AS [Invoice due date key]
    , t.LegalEntityKey                                                                                                    AS [Legal entity key]
    , t.ProductKey                                                                                                        AS [Product key]
    , solf.SalesOrderKey                                                                                                  AS [Sales order key]
    , t.SalesOrderLineKey                                                                                                 AS [Sales order line key]
    , t.SalesPersonKey                                                                                                    AS [Sales person key]
    , CAST(1 AS INT)                                                                                                      AS [Sales invoice line count]
    , t.BaseAmount                                                                                                        AS [Base amount]
    , t.BaseAmount_TransCur                                                                                               AS [Base amount in trans currency]
    , t.BaseUnitPrice                                                                                                     AS [Invoice base unit price]
    , t.BaseUnitPrice_TransCur                                                                                            AS [Invoice base unit price in trans currency]
    , CASE WHEN st.SalesType <> 'Returned order' THEN t.NetAmount ELSE 0 END                                              AS [Credit sales]
    , CASE WHEN st.SalesType <> 'Returned order' THEN t.NetAmount_TransCur ELSE 0 END                                     AS [Credit sales in trans currency]
    , t.DiscountAmount                                                                                                    AS [Invoice discount]
    , t.DiscountAmount_TransCur                                                                                           AS [Invoice discount in trans currency]
    , t.CostAmount / ISNULL(NULLIF(t.InvoiceQuantity_SalesUOM, 0), 1)                                                     AS [Invoice unit cost]
    , t.CostAmount_TransCur / ISNULL(NULLIF(t.InvoiceQuantity, 0), 1)                                                     AS [Invoice unit cost in trans currency]
    , t.NetAmount                                                                                                         AS [Invoice net amount]
    , t.NetAmount_TransCur                                                                                                AS [Invoice net amount in trans currency]
    , ISNULL(silcf.NonBillableCharge * -1, 0)                                                                             AS [Invoice non-billable charges]
    , ISNULL(silcf.NonBillableCharge_TransCur * -1, 0)                                                                    AS [Invoice non-billable charges in trans currency]
    , t.PriceUnit                                                                                                         AS [Invoice price unit]
    , t.InvoiceQuantity_SalesUOM                                                                                          AS [Invoice quantity]
    , t.InvoiceQuantity_LB * 1 AS [Invoice LB], t.InvoiceQuantity_LB * 0.01 AS [Invoice CWT], t.InvoiceQuantity_LB * 0.0005 AS [Invoice TON]
    , t.InvoiceSalesAmount                                                                                                AS [Invoice sales amount]
    , t.InvoiceSalesAmount_TransCur                                                                                       AS [Invoice sales amount in trans currency]
    , t.CostAmount                                                                                                        AS [Invoice sales cost]
    , t.CostAmount_TransCur                                                                                               AS [Invoice sales cost in trans currency]
    , t.TaxAmount                                                                                                         AS [Invoice tax]
    , t.TaxAmount_TransCur                                                                                                AS [Invoice tax in trans currency]
    , ISNULL(t.CustomerCharge, 0)                                                                                         AS [Invoice total charges]
    , ISNULL(t.CustomerCharge_TransCur, 0)                                                                                AS [Invoice total charges in trans currency]
    , t.InvoiceTotalAmount                                                                                                AS [Invoice total]
    , t.InvoiceTotalAmount_TransCur                                                                                       AS [Invoice total in trans currency]
    , t.TotalUnitPrice                                                                                                    AS [Invoice total unit price]
    , t.TotalUnitPrice_TransCur                                                                                           AS [Invoice total unit price in trans currency]
    , CASE WHEN st.SalesType = 'Returned order' THEN NULL ELSE t.BaseUnitPrice - solf.BaseUnitPrice END                   AS [Sales price variance]
    , CASE WHEN st.SalesType = 'Returned order' THEN NULL ELSE t.BaseUnitPrice_TransCur - solf.BaseUnitPrice_TransCur END AS [Sales price variance in trans currency]
  FROM {{ ref("SalesInvoiceLine_Fact") }}    t 
LEFT JOIN {{ ref("SalesInvoice") }}        si
    ON si.SalesInvoiceKey        = t.SalesInvoiceKey
  LEFT JOIN Charges                 silcf 
    ON silcf.SalesInvoiceLineKey = t.SalesInvoiceLineKey
  LEFT JOIN {{ ref("SalesType") }}           st
    ON st.SalesTypeKey           = t.SalesTypeKey
  LEFT JOIN {{ ref("SalesOrderLine_Fact") }} solf
    ON solf.SalesOrderLineKey    = t.SalesOrderLineKey
