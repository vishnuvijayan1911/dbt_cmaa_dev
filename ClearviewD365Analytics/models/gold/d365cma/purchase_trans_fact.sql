{{ config(materialized='view', schema='gold', alias="Purchase trans fact") }}

SELECT  ROW_NUMBER() OVER (ORDER BY COALESCE(poltf.PurchaseOrderLineTransKey, -1)
                                , COALESCE(prltf.ProductReceiptLineTransKey, -1)
                                , COALESCE(piltf.PurchaseInvoiceLineTransKey, -1))                        AS [Purchase trans fact key]
    , COALESCE(polf.OrderDateKey, prlf.ReceiptDateKey, pilf.InvoiceDateKey, prl.CreatedDateKey, 19000101) AS [Report date key]
    , COALESCE(poltf.PurchaseOrderLineTransKey, -1)                                                       AS [Purchase order line trans key]
    , COALESCE(prltf.ProductReceiptLineTransKey, -1)                                                      AS [Product receipt line trans key]
    , COALESCE(piltf.PurchaseInvoiceLineTransKey, -1)                                                     AS [Purchase invoice line trans key]
    , COALESCE(prltf.InventoryTransStatusKey, poltf.InventoryTransStatusKey, -1)                          AS [Inventory trans status key]
    , COALESCE(piltf.PurchaseInvoiceLineKey, -1)                                                          AS [Purchase invoice line key]
    , COALESCE(prltf.ProductReceiptLineKey, -1)                                                           AS [Product receipt line key]
    , COALESCE(polf.PurchaseOrderLineKey, -1)                                                             AS [Purchase order line key]
    , COALESCE(prl.PurchaseRequisitionLineKey, -1)                                                        AS [Purchase requisition line key]
    , COALESCE(piltf.TagKey, prltf.TagKey, poltf.TagKey, -1)                                              AS [Tag key]
    , COALESCE(pilf.FinancialKey, prlf.FinancialKey, polf.FinancialKey, -1)                               AS [Financial key]
    , COALESCE(pilf.VendorKey, prlf.VendorKey, polf.VendorKey, prl.VendorKey, -1)                         AS [Vendor key]
    , COALESCE(pilf.DeliveryAddressKey, prlf.DeliveryAddressKey, polf.DeliveryAddressKey, -1)             AS [Delivery address key]
    , COALESCE(pilf.InvoiceVendorKey, prlf.InvoiceVendorKey, polf.InvoiceVendorKey, -1)                   AS [Invoice vendor key]
    , COALESCE(pilf.LegalEntityKey, polf.LegalEntityKey, prl.LegalEntityKey, -1)                          AS [Legal entity key]
    , COALESCE(pilf.ProductKey, prlf.ProductKey, polf.ProductKey, prl.ProductKey, -1)                     AS [Product key]
    , COALESCE(polf.OrderDateKey, 19000101)                                                               AS [Order date key]
    , COALESCE(prlf.ReceiptDateKey, 19000101)                                                             AS [Receive date key]
    , COALESCE(pilf.InvoiceDateKey, 19000101)                                                             AS [Invoice date key]
    , COALESCE(pilf.InventorySiteKey, prlf.InventorySiteKey, polf.InventorySiteKey, -1)                   AS [Inventory site key]
    , COALESCE(polf.SalesOrderLineKey, -1)                                                                AS [Sales order line key]
    , COALESCE(solf.CustomerKey, -1)                                                                      AS [Customer key]
    , COALESCE(pilf.WarehouseKey, prlf.WarehouseKey, polf.WarehouseKey, -1)                               AS [Warehouse key]
    , CAST(1 AS INT)                                                                                      AS [Purchase trans count]
    , COALESCE(pold.Date, prld.Date, pild.Date, prd.Date)                                                 AS [Report date]
  FROM {{ ref("purchaseorderlinetrans_fact") }}              poltf 
INNER JOIN {{ ref("purchaseorderline_fact") }}              polf 
    ON polf.PurchaseOrderLineKey        = poltf.PurchaseOrderLineKey
  LEFT JOIN {{ ref('date') }}                                pold 
    ON pold.DateKey                     = polf.OrderDateKey
  FULL OUTER JOIN {{ ref("productreceiptlinetrans_fact") }}  prltf 
    ON prltf.PurchaseOrderLineTransKey  = poltf.PurchaseOrderLineTransKey
  LEFT JOIN {{ ref("productreceiptline_fact") }}             prlf 
    ON prlf.ProductReceiptLineKey       = prltf.ProductReceiptLineKey
  LEFT JOIN {{ ref('date') }}                                prld 
    ON prld.DateKey                     = prlf.ReceiptDateKey
  FULL OUTER JOIN {{ ref("purchaseinvoicelinetrans_fact") }} piltf 
    ON piltf.ProductReceiptLineTransKey = prltf.ProductReceiptLineTransKey
  LEFT JOIN {{ ref("purchaseinvoiceline_fact") }}            pilf 
    ON pilf.PurchaseInvoiceLineKey      = piltf.PurchaseInvoiceLineKey
  LEFT JOIN {{ ref('date') }}                                pild 
    ON pild.DateKey                     = pilf.InvoiceDateKey
  LEFT JOIN {{ ref("inventorytransstatus") }}                ist 
    ON ist.InventoryTransStatusKey      = poltf.InventoryTransStatusKey
  LEFT JOIN {{ ref("purchasetype") }}                        st 
    ON st.PurchaseTypeKey               = polf.PurchaseTypeKey
  FULL OUTER JOIN {{ ref("purchaserequisitionline_f") }}  prl 
    ON prl.PurchaseRequisitionLineKey   = polf.PurchaseRequisitionLineKey
  LEFT JOIN {{ ref('date') }}                                Receiveddate
    ON Receiveddate.DateKey             = prlf.ReceiptDateKey
  LEFT JOIN {{ ref('date') }}                                prd 
    ON prd.DateKey                      = prl.CreatedDateKey
  LEFT JOIN {{ ref("salesorderline_fact") }}                 solf
    ON solf.SalesOrderLineKey           = polf.SalesOrderLineKey;
