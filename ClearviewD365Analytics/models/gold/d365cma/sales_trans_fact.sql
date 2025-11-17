{{ config(materialized='view', schema='gold', alias="Sales trans fact") }}

SELECT  ROW_NUMBER() OVER (ORDER BY COALESCE(soltf.SalesOrderLineTransKey, -1)
                                , COALESCE(psltf.PackingSlipLineTransKey, -1)
                                , COALESCE(siltf.SalesInvoiceLineTransKey, -1))                                 AS [Sales trans fact key]
    , COALESCE(solf.OrderDateKey, pslf.PackingSlipDateKey, silf.InvoiceDateKey, -1)                             AS [Report date key]
    , COALESCE(soltf.SalesOrderLineTransKey, -1)                                                                AS [Sales order line trans key]
    , COALESCE(psltf.PackingSlipLineTransKey, -1)                                                               AS [Packing slip line trans key]
    , COALESCE(siltf.SalesInvoiceLineTransKey, -1)                                                              AS [Sales invoice line trans key]
    , COALESCE(psltf.InventoryTransStatusKey, soltf.InventoryTransStatusKey, siltf.InventoryTransStatusKey, -1) AS [Inventory trans status key]
    , COALESCE(pslf.OnTimeShipStatusKey, -1)                                                                    AS [On-time ship status key]
    , COALESCE(siltf.SalesInvoiceLineKey, -1)                                                                   AS [Sales invoice line key]
    , COALESCE(psltf.PackingSlipLineKey, -1)                                                                    AS [Packing slip line key]
    , COALESCE(soltf.AgingReservedBucketKey, -1)                                                                AS [Aging reserved bucket key]
    , COALESCE(psltf.MasterProductKey, -1)                                                                      AS [Master product key]
    , COALESCE(psltf.MasterTagKey, -1)                                                                          AS [Master tag key]
    , COALESCE(psltf.ParentProductKey, solf.BOMParentProductKey, -1)                                       AS [Parent product key]
    , COALESCE(psltf.ParentTagKey, -1)                                                                          AS [Parent tag key]
    , COALESCE(solf.PricingUOMKey, -1)                                                                          AS [Pricing UOM key]
    , COALESCE(solf.SalesOrderLineKey, -1)                                                                      AS [Sales order line key]
    , COALESCE(psltf.TagKey, siltf.TagKey, soltf.TagKey, -1)                                                    AS [Tag key]
    , COALESCE(silf.CustomerKey, pslf.CustomerKey, solf.CustomerKey, -1)                                        AS [Customer key]
    , COALESCE(silf.DeliveryAddressKey, pslf.DeliveryAddressKey, solf.DeliveryAddressKey, -1)                   AS [Delivery address key]
    , COALESCE(silf.FinancialKey, pslf.FinancialKey, solf.FinancialKey, -1)                                     AS [Financial key]
    , COALESCE(silf.InvoiceCustomerKey, pslf.InvoiceCustomerKey, solf.InvoiceCustomerKey, -1)                   AS [Invoice customer key]
    , COALESCE(silf.LegalEntityKey, pslf.LegalEntityKey, solf.LegalEntityKey, -1)                               AS [Legal entity key]
    , COALESCE(pslf.ShippingLoadKey, -1)                                                                        AS [Load key]
    , COALESCE(silf.ProductKey, pslf.ProductKey, solf.ProductKey, -1)                                           AS [Product key]
    , COALESCE(solf.ProductionKey, -1)                                                                          AS [Production key]
    , COALESCE(silf.SalesCategoryKey, solf.SalesCategoryKey, -1)                                                AS [Sales category key]
    , COALESCE(solf.OrderDateKey, -1)                                                                           AS [Created date key]
    , COALESCE(pslf.PackingSlipDateKey, -1)                                                                     AS [Packing slip date key]
    , COALESCE(silf.InvoiceDateKey, -1)                                                                         AS [Invoice date key]
    , COALESCE(silf.InventorySiteKey, pslf.InventorySiteKey, solf.InventorySiteKey, -1)                         AS [Inventory site key]
    , COALESCE(silf.WarehouseKey, pslf.WarehouseKey, solf.WarehouseKey, -1)                                     AS [Warehouse key]
    , CAST(1 AS INT)                                                                                            AS [Trans count]
    , COALESCE(sold.Date, psld.Date, sild.Date)                                                                 AS [Report date]
  FROM {{ ref("salesorderlinetrans_fact") }}              soltf 
  LEFT JOIN {{ ref("salesorderline_fact") }}              solf 
    ON solf.SalesOrderLineKey        = soltf.SalesOrderLineKey
  LEFT JOIN {{ ref('date') }}                             sold 
    ON sold.DateKey                  = solf.OrderDateKey
  FULL OUTER JOIN {{ ref("packingsliplinetrans_fact") }}  psltf 
    ON psltf.SalesOrderLineTransKey  = soltf.SalesOrderLineTransKey
  LEFT JOIN {{ ref("packingslipline_fact") }}             pslf 
    ON pslf.PackingSlipLineKey       = psltf.PackingSlipLineKey
  LEFT JOIN {{ ref('date') }}                             psld 
    ON psld.DateKey                  = pslf.PackingSlipDateKey
  FULL OUTER JOIN {{ ref("salesinvoicelinetrans_fact") }} siltf 
    ON siltf.SalesOrderLineTransKey = soltf.SalesOrderLineTransKey
  LEFT JOIN {{ ref("salesinvoiceline_fact") }}            silf 
    ON silf.SalesInvoiceLineKey      = siltf.SalesInvoiceLineKey
  LEFT JOIN {{ ref('date') }}                             sild 
    ON sild.DateKey                  = silf.InvoiceDateKey
  LEFT JOIN {{ ref('date') }}                             dd 
    ON dd.DateKey                    = solf.ShipDateDueKey
  LEFT JOIN {{ ref("inventorytransstatus") }}             ist 
    ON ist.InventoryTransStatusKey   = soltf.InventoryTransStatusKey
  LEFT JOIN {{ ref("salestype") }}                        st 
    ON st.SalesTypeKey               = solf.SalesTypeKey
WHERE (soltf._SourceID = 1 OR psltf._SourceID = 1 OR siltf._SourceID = 1)
  AND NOT (ISNULL(soltf.SalesOrderLineTransKey, -1) = -1 AND ISNULL(psltf.PackingSlipLineTransKey, -1) <> -1);
