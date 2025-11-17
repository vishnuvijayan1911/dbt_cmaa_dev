{{ config(materialized='view', schema='gold', alias="Product receipt line") }}

SELECT  t.ProductReceiptLineKey                                                       AS [Product receipt line key]
  , NULLIF(t.ReceiptID, '')                                                       AS [Receipt #]
  , NULLIF(t.IsFullyMatched, '')                                                  AS [Is fully matched]
  , NULLIF(t.LineNumber, '')                                                      AS [Line #]
  , CASE WHEN t.ProductReceiptLineKey <> -1 THEN CAST(1 AS SMALLINT)ELSE NULL END AS [Receipt lines]
  , NULLIF(dv.VoucherID, '')                                                      AS [Physical voucher #]
  , NULLIF(dd.Date, '1/1/1900')                                                   AS [Receipt date]
FROM {{ ref("productreceiptline") }}           t 
LEFT JOIN {{ ref("productreceiptline_fact") }} f 
  ON f.ProductReceiptLineKey = t.ProductReceiptLineKey
LEFT JOIN {{ ref('date') }}                    dd 
  ON dd.DateKey              = f.ReceiptDateKey
LEFT JOIN {{ ref("voucher") }}                 dv 
  ON dv.VoucherKey           = f.VoucherKey;
