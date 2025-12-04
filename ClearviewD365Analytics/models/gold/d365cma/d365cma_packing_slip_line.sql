{{ config(materialized='view', schema='gold', alias="Packing slip line") }}

SELECT  t.PackingSlipLineKey                                                       AS [Packing slip line key]
  , NULLIF(t.PackingSlipID, '')                                                AS [Packing slip #]
  , NULLIF(t.LineNumber, '')                                                   AS [Line #]
  , NULLIF(ots.OnTimeShipStatus, '')                                           AS [Packing slip ship on-time status]
  , CASE WHEN t.PackingSlipLineKey <> -1 THEN CAST(1 AS SMALLINT)ELSE NULL END AS [Packing slip lines]
  , NULLIF(dv.VoucherID, '')                                                   AS [Physical voucher #]
  , NULLIF(dd.Date, '1/1/1900')                                                AS [Ship date]
FROM {{ ref("d365cma_packingslipline_d") }}           t 
LEFT JOIN {{ ref("d365cma_packingslipline_f") }} f 
  ON f.PackingSlipLineKey = t.PackingSlipLineKey
LEFT JOIN {{ ref('d365cma_date_d') }}                 dd 
  ON dd.DateKey           = f.PackingSlipDateKey
LEFT JOIN {{ ref("d365cma_voucher_d") }}              dv 
  ON dv.VoucherKey        = f.VoucherKey
LEFT JOIN {{ ref("d365cma_ontimeshipstatus_d") }}    ots 
  ON ots.OnTimeShipStatusKey  = F.OnTimeShipStatusKey;
