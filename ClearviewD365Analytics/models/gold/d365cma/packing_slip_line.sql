{{ config(materialized='view', schema='gold', alias="Packing slip line") }}

SELECT  t.PackingSlipLineKey                                                       AS [Packing slip line key]
  , NULLIF(t.PackingSlipID, '')                                                AS [Packing slip #]
  , NULLIF(t.LineNumber, '')                                                   AS [Line #]
  , NULLIF(ots.OnTimeShipStatus, '')                                           AS [Packing slip ship on-time status]
  , CASE WHEN t.PackingSlipLineKey <> -1 THEN CAST(1 AS SMALLINT)ELSE NULL END AS [Packing slip lines]
  , NULLIF(dv.VoucherID, '')                                                   AS [Physical voucher #]
  , NULLIF(dd.Date, '1/1/1900')                                                AS [Ship date]
FROM {{ ref("PackingSlipLine") }}           t 
LEFT JOIN {{ ref("PackingSlipLine_Fact") }} f 
  ON f.PackingSlipLineKey = t.PackingSlipLineKey
LEFT JOIN {{ ref("Date") }}                 dd 
  ON dd.DateKey           = f.PackingSlipDateKey
LEFT JOIN {{ ref("Voucher") }}              dv 
  ON dv.VoucherKey        = f.VoucherKey
LEFT JOIN {{ ref("OnTimeShipStatus") }}    ots 
  ON ots.OnTimeShipStatusKey  = F.OnTimeShipStatusKey;
