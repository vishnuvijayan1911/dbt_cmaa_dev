{{ config(materialized='view', schema='gold', alias="Packing slip trans fact") }}

SELECT  psltf.PackingSlipLineTransKey                   AS [Packing slip line trans key]
    , COALESCE(pslf.PackingSlipDateKey, 19000101) AS [Packing slip date key]
    , CAST(1 AS INT)                                   AS [Packing slip trans count]
    , psltf.ShippedAmount                              AS [Ship amount]
    , psltf.ShippedAmount_TransCur                     AS [Ship amount in trans currency]
    , psltf.ShippedQuantity_SalesUOM                   AS [Ship quantity]
      , psltf.ShippedQuantity_LB * 1 AS [Ship LB], psltf.ShippedQuantity_LB * 0.01 AS [Ship CWT], psltf.ShippedQuantity_LB * 0.0005 AS [Ship TON]
      , psltf.ShippedQuantity_PC * 1 AS [Ship PC]
    , psltf.ShippedNotInvoicedAmount                   AS [Ship not invoiced amount]
    , psltf.ShippedNotInvoicedAmount_TransCur          AS [Ship not invoiced amount in trans currency]
    , NULLIF(psltf.ShippedNotInvoicedDays, 0)          AS [Ship not invoiced days]
    , psltf.ShippedNotInvoiceTransCount                AS [Ship not invoice trans]
    , psltf.ShippedNotInvoicedQuantity_SalesUOM        AS [Ship not invoiced quantity]
    , psltf.ShippedNotInvoicedQuantity_LB * 1 AS [Ship not invoiced LB], psltf.ShippedNotInvoicedQuantity_LB * 0.01 AS [Ship not invoiced CWT], psltf.ShippedNotInvoicedQuantity_LB * 0.0005 AS [Ship not invoiced TON]
    , psltf.ShippedNotInvoicedQuantity_PC * 1 AS [Ship not invoiced PC]
  FROM {{ ref("PackingSlipLineTrans_Fact") }}     psltf 
INNER JOIN {{ ref("PackingSlipLine_Fact") }} pslf 
    ON pslf.PackingSlipLineKey = psltf.PackingSlipLineKey
WHERE psltf._SourceID = 1
UNION ALL
SELECT  -1             AS [Packing slip line trans key]
    , -1             AS [Packing slip date key]
    , CAST(0 AS INT) AS [Packing slip trans count]
    , NULL              AS [Ship amount]
    , NULL              AS [Ship amount in trans currency]
    , NULL              AS [Ship quantity]
    , 0 AS [Ship LB], 0 AS [Ship CWT], 0 AS [Ship TON]
    , 0 AS [Ship PC]
    , NULL              AS [Ship not invoiced amount]
    , NULL              AS [Ship not invoiced amount in trans currency]
    , NULL              AS [Ship not invoiced days]
    , NULL              AS [Ship not invoice trans]
    , NULL              AS [Ship not invoiced quantity]
      , 0 AS [Ship not invoiced LB], 0 AS [Ship not invoiced CWT], 0 AS [Ship not invoiced TON]
      , 0 AS [Ship not invoiced PC];
