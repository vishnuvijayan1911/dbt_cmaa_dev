{{ config(materialized='view', schema='gold', alias="Product price fact") }}

SELECT  f.ProductPriceKey           AS [Product price key]
    , f.LegalEntityKey            AS [Legal entity key]
    , f.ProductKey                AS [Product key]
    , f.FromDateKey               AS [From date key]
    , f.ToDateKey                 AS [To date key]
    , COALESCE(f.StandardPrice, '') AS [Standard price]
    , dd.Date                     AS [From date]
    , dd1.Date                    AS [To date]
  FROM {{ ref("productprice_fact") }}   f
  JOIN {{ ref('date') }}                dd 
    ON dd.DateKey            = f.FromDateKey
  JOIN {{ ref('date') }}                dd1 
    ON dd1.DateKey           = f.ToDateKey;
