{{ config(materialized='view', schema='gold', alias="Load") }}

SELECT  t.ShippingLoadKey                                                                                   AS [Load key]
    , t.LegalEntityID                                                                                     AS [Legal entity ID]
    , t.CarrierService                                                                                    AS [Carrier service]
    , t.LoadID                                                                                            AS [Load #]
    , t.LoadStatus                                                                                        AS [Load status]
    , ls.OnTimeLoadStatus                                                                                 AS [Load on-time ship status]
    , ls.OnTimeStatus                                                                                     AS [Load on-time status]
    , ls.ShipStatus                                                                                       AS [Load ship status]
    , CASE WHEN t.OnTimeLoadStatusKey <> -1 THEN CAST(1 AS INT)ELSE NULL END                              AS [Loads]
    , CASE WHEN ls.OnTimeStatus = 'On-time' AND t.OnTimeLoadStatusKey <> -1 THEN 1 ELSE 0 END             AS [Loads on-time]
    , CASE WHEN ls.OnTimeStatus = 'Late' AND t.OnTimeLoadStatusKey <> -1 THEN 1 ELSE 0 END                AS [Loads late]
    , CASE WHEN ls.OnTimeLoadStatus = 'Shipped late' AND t.OnTimeLoadStatusKey <> -1 THEN 1 ELSE 0 END    AS [Loads ship late]
    , CASE WHEN ls.ShipStatus = 'Shipped' AND t.OnTimeLoadStatusKey <> -1 THEN 1 ELSE 0 END               AS [Loads shipped]
    , CASE WHEN ls.OnTimeLoadStatus = 'Shipped on-time' AND t.OnTimeLoadStatusKey <> -1 THEN 1 ELSE 0 END AS [Loads ship on-time]
    , t.ShippingCarrier                                                                                   AS [Ship carrier]
    , NULLIF(t.LoadShippedConfirmationDate, '1/1/1900')                                                   AS [Load ship date confirmed]
    , NULLIF(t.ScheduledLoadShippingDate, '1/1/1900')                                                     AS [Load ship date scheduled]
  FROM {{ ref("ShippingLoad") }}          t 
  LEFT JOIN {{ ref("OnTimeLoadStatus") }} ls
    ON ls.OnTimeLoadStatusKey = t.OnTimeLoadStatusKey;
