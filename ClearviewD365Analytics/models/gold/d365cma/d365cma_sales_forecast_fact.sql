{{ config(materialized='view', schema='gold', alias="Sales forecast fact") }}

SELECT  t.SalesForecastKey           AS [Sales forecast key]
    , CAST(1 AS INT)                AS [Sales forecast count]
    , t.CurrencyKey                AS [Currency key]
    , t.CustomerKey                AS [Customer key]
    , t.FinancialKey               AS [Financial key]
    , t.ForecastDateKey            AS [Forecast date key]
    , t.InventorySiteKey           AS [Inventory site key]
    , t.LegalEntityKey             AS [Legal entity key]
    , t.ProductKey                 AS [Product key]
    , t.SalesForecastModelKey      AS [Sales forecast model key]
    , t.SalesPersonKey             AS [Sales person key]
    , t.WarehouseKey               AS [Warehouse key]
    , t.ForecastAmount             AS [Forecast amount]
    , t.ForecastQuantity           AS [Forecast quantity]
    , t.ForecastQuantity_LB * 1 AS [Forecast LB], t.ForecastQuantity_LB * 0.01 AS [Forecast CWT], t.ForecastQuantity_LB * 0.0005 AS [Forecast TON]
    , t.ForecastQuantity_PC * 1 AS [Forecast PC]
    , t.ForecastQuantity_FT * 1 AS [Forecast FT], t.ForecastQuantity_FT * 12 AS [Forecast IN]
    , t.ForecastQuantity_SQIN * 1 AS [Forecast SQIN]
    , t.ForecastQuantity_SalesUOM  AS [Sales forecast quantity]  
    , t.ForecastQuantity_SalesUOM_LB * 1 AS [Sales forecast LB], t.ForecastQuantity_SalesUOM_LB * 0.01 AS [Sales forecast CWT], t.ForecastQuantity_SalesUOM_LB * 0.0005 AS [Sales forecast TON]
    , t.ForecastQuantity_SalesUOM_PC * 1 AS [Sales forecast PC]
    , t.ForecastQuantity_SalesUOM_FT * 1 AS [Sales forecast FT], t.ForecastQuantity_SalesUOM_FT * 12 AS [Sales forecast IN]
    , t.ForecastQuantity_SalesUOM_SQIN * 1 AS [Sales forecast SQIN]
    , t.PriceUnit                  AS [Price unit]
    , t.SalesPrice                 AS [Sales price]
    , NULLIF(cur.CurrencyID, '')   AS [Trans currency]
    , NULLIF(du.UOM, '')           AS [Sales UOM]
    , NULLIF(dd.Date, '1/1/1900')  AS [Expected cost payment date]
    , NULLIF(dd1.Date, '1/1/1900') AS [Expected invoice date]
    , NULLIF(dd2.Date, '1/1/1900') AS [Expected sales payment date]
  FROM {{ ref("d365cma_salesforecast_f") }} t 
INNER JOIN {{ ref("d365cma_currency_d") }}      cur 
    ON cur.CurrencyKey = t.CurrencyKey
INNER JOIN {{ ref("d365cma_uom_d") }}           du 
    ON du.UOMKey       = t.SalesUOMKey
INNER JOIN {{ ref('d365cma_date_d') }}          dd 
    ON dd.DateKey      = t.ExpectedCostPaymentDateKey
INNER JOIN {{ ref('d365cma_date_d') }}          dd1 
    ON dd1.DateKey     = t.ExpectedInvoiceDateKey
INNER JOIN {{ ref('d365cma_date_d') }}          dd2 
    ON dd2.DateKey     = t.ExpectedSalesPaymentDateKey;
