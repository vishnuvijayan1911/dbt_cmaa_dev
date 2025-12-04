{{ config(materialized='view', schema='gold', alias="Delivery address") }}

SELECT  t.AddressKey                AS [Delivery address key]
    , NULLIF(t.City, '')          AS [Delivery city]
    , NULLIF(t.CountryID, '')     AS [Delivery country]
    , NULLIF(t.Country, '')       AS [Delivery country name]
    , NULLIF(t.StateProvince, '') AS [Delivery state province]
    , NULLIF(t.Street, '')        AS [Delivery street]
    , NULLIF(t.PostalCode, '')    AS [Delivery postal code]
FROM {{ ref("d365cma_address_d") }} t;
