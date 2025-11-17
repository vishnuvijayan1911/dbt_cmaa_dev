{{ config(materialized='view', schema='gold', alias="Sales quote line") }}

SELECT  t.SalesQuoteLineKey                                                                            AS [Sales quote line key]
    , NULLIF(t.QuoteID, '')                                                                           AS [Sales quote #]
    , NULLIF(t.LineNumber, '')                                                                        AS [Line #]
    , NULLIF(t.QuoteName, '')                                                                         AS [Quote name]
    , NULLIF(f.CustomerReference, '')                                                                 AS [Customer reference]
    , CASE WHEN dd.Date < CAST(GETDATE() AS DATE) THEN NULL ELSE DATEDIFF(dd, GETDATE(), dd.Date) END AS [Days until expiration]
    , NULLIF(dm.DeliveryModeID, '')                                                                   AS [Delivery mode]
    , NULLIF(dm.DeliveryMode, '')                                                                     AS [Delivery mode name]
    , NULLIF(dt.DeliveryTermID, '')                                                                   AS [Delivery term]
    , NULLIF(dt.DeliveryTerm, '')                                                                     AS [Delivery term name]
    , CASE WHEN dd.Date < CAST(GETDATE() AS DATE) THEN 'Expired' ELSE 'Not Expired' END               AS [Expiration status]
    , NULLIF(pm.PaymentModeID, '')                                                                    AS [Payment mode]
    , NULLIF(pm.PaymentMode, '')                                                                      AS [Payment mode name]
    , NULLIF(pat.PaymentTerm, '')                                                                     AS [Payment term]
    , NULLIF(qs.QuoteStatus, '')                                                                      AS [Quote status]
    , NULLIF(qt.QuoteType, '')                                                                        AS [Quote type]
    , NULLIF(du.UOM, '')                                                                              AS [Quote UOM]
    , NULLIF(du1.UOM, '')                                                                             AS [Quote price UOM]
    , NULLIF(sp.SalesPerson, '')                                                                      AS [Sales person]
    , NULLIF(emp2.EmployeeName, '')                                                                   AS [Sales taker]
    , NULLIF(c.CurrencyID, '')                                                                        AS [Trans currency]
    , NULLIF(dd.Date, '1/1/1900')                                                                     AS [Expiration date]
    , NULLIF(dd1.Date, '1/1/1900')                                                                    AS [Quote date]
    , NULLIF(dd2.Date, '1/1/1900')                                                                    AS [Receipt date requested]
    , NULLIF(dd3.Date, '1/1/1900')                                                                    AS [Ship date requested]
  FROM {{ ref("salesquoteline_d") }}           t 
  -- INNER JOIN DateFilter                       df
  --   ON 1                            = 1
  LEFT JOIN {{ ref("salesquoteline_f") }} f 
    ON f.SalesQuoteLineKey = t.SalesQuoteLineKey
  LEFT JOIN {{ ref("currency_d") }}            c 
    ON c.CurrencyKey       = f.CurrencyKey
  LEFT JOIN {{ ref("deliverymode_d") }}        dm 
    ON dm.DeliveryModeKey  = f.DeliveryModeKey
  LEFT JOIN {{ ref("deliveryterm_d") }}        dt 
    ON dt.DeliveryTermKey  = f.DeliveryTermKey
  LEFT JOIN {{ ref("employee_d") }}            emp2 
    ON emp2.EmployeeKey    = f.SalesTakerKey
  LEFT JOIN {{ ref("paymentmode_d") }}         pm 
    ON pm.PaymentModeKey   = f.PaymentModeKey
  LEFT JOIN {{ ref("paymentterm_d") }}         pat 
    ON pat.PaymentTermKey  = f.PaymentTermKey
  LEFT JOIN {{ ref("quotestatus_d") }}         qs 
    ON qs.QuoteStatusKey   = f.QuoteStatusKey
  LEFT JOIN {{ ref("quotetype_d") }}           qt 
    ON qt.QuoteTypeKey     = f.QuoteTypeKey
  LEFT JOIN {{ ref('date_d') }}                dd 
    ON dd.DateKey          = f.ExpirationDateKey
  LEFT JOIN {{ ref('date_d') }}                dd1 
    ON dd1.DateKey         = f.QuoteDateKey
  LEFT JOIN {{ ref('date_d') }}                dd2 
    ON dd2.DateKey         = f.ReceiptDateRequestedKey
  LEFT JOIN {{ ref('date_d') }}                dd3 
    ON dd3.DateKey         = f.ShipDateRequestedKey
  LEFT JOIN {{ ref("uom_d") }}                 du 
    ON du.UOMKey           = f.SalesUOMKey
  LEFT JOIN {{ ref("uom_d") }}                 du1 
    ON du1.UOMKey           = f.QuotePriceUOMKey
  LEFT JOIN {{ ref("salesperson_d") }}         sp 
    ON sp.SalesPersonKey   = f.SalesPersonKey;
