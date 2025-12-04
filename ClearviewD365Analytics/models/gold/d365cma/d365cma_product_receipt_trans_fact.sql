{{ config(materialized='view', schema='gold', alias="Product receipt trans fact") }}

SELECT  prltf.ProductReceiptLineTransKey                      AS [Product receipt line trans key]
    , COALESCE(prlf.ReceiptDateKey, -1)                     AS [Receive date key]
    , CAST(1 AS INT)                                        AS [Product receipt trans count]
    , ISNULL(prltf.ReceivedAmount, 0)                       AS [Receive amount]
    , ISNULL(prltf.ReceivedAmount_TransCur, 0)              AS [Receive amount in trans currency]
    , ISNULL(prltf.ReceivedQuantity_PurchUOM, 0)            AS [Receive quantity]
    , ISNULL(prltf.ReceivedQuantity_LB, 0) * 1 AS [Receive LB], ISNULL(prltf.ReceivedQuantity_LB, 0) * 0.01 AS [Receive CWT], ISNULL(prltf.ReceivedQuantity_LB, 0) * 0.0005 AS [Receive TON]
    , ISNULL(prltf.ReceivedQuantity_PC, 0) * 1 AS [Receive PC]
    , ISNULL(prltf.ReceivedNotInvoicedAmount, 0)            AS [Receive not invoiced amount]
    , ISNULL(prltf.ReceivedNotInvoicedAmount_TransCur, 0)   AS [Receive not invoiced amount in trans currency]
    , NULLIF(prltf.ReceivedNotInvoicedDays, 0)              AS [Receive not invoiced days]
    , ISNULL(prltf.ReceivedNotInvoiceTransCount, 0)         AS [Receive not invoice trans]
    , ISNULL(prltf.ReceivedNotInvoicedQuantity_PurchUOM, 0) AS [Receive not invoiced quantity]
    , ISNULL(prltf.ReceivedNotInvoicedQuantity_LB, 0) * 1 AS [Receive not invoiced LB], ISNULL(prltf.ReceivedNotInvoicedQuantity_LB, 0) * 0.01 AS [Receive not invoiced CWT], ISNULL(prltf.ReceivedNotInvoicedQuantity_LB, 0) * 0.0005 AS [Receive not invoiced TON]
    , ISNULL(prltf.ReceivedNotInvoicedQuantity_PC, 0) * 1 AS [Receive not invoiced PC]
  FROM {{ ref("d365cma_productreceiptlinetrans_f") }} prltf 
INNER JOIN {{ ref("d365cma_productreceiptline_f") }} prlf 
    ON prlf.ProductReceiptLineKey     = prltf.ProductReceiptLineKey
UNION ALL
SELECT  -1             AS [Product receipt line trans key]
    , -1             AS [Receive date key]
    , CAST(0 AS INT) AS [Product receipt trans count]
    , NULL              AS [Receive amount]
    , NULL              AS [Receive amount in trans currency]
    , NULL              AS [Receive quantity]
    , 0 AS [Receive LB], 0 AS [Receive CWT], 0 AS [Receive TON]
    , 0 AS [Receive PC]
    , NULL              AS [Receive not invoiced amount]
    , NULL              AS [Receive not invoiced amount in trans currency]
    , NULL              AS [Receive not invoiced days]
    , NULL              AS [Receive not invoice trans]
    , NULL              AS [Receive not invoiced quantity]
    , 0 AS [Receive not invoiced LB], 0 AS [Receive not invoiced CWT], 0 AS [Receive not invoiced TON]
    , 0 AS [Receive not invoiced PC];
