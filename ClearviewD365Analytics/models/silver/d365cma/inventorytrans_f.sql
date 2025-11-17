{{ config(materialized='table', tags=['silver'], alias='inventorytrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/inventorytrans_f/inventorytrans_f.py
-- Root method: InventorytransFact.inventorytrans_factdetail [InventoryTrans_FactDetail]
-- Inlined methods: InventorytransFact.inventorytrans_factpurchase [InventoryTrans_FactPurchase], InventorytransFact.inventorytrans_factsales [InventoryTrans_FactSales], InventorytransFact.inventorytrans_factfindim [InventoryTrans_FactFinDim], InventorytransFact.inventorytrans_factstage [InventoryTrans_FactStage], InventorytransFact.inventorytrans_factinventory [InventoryTrans_FactInventory], InventorytransFact.inventorytrans_factdetailmain [InventoryTrans_FactDetailMain]
-- external_table_name: InventoryTrans_FactDetail
-- schema_name: temp

WITH
inventorytrans_factpurchase AS (
    SELECT DISTINCT
               it.recid         AS RecID_IT
             , MAX (vit.recid) AS RecID_VIT

          FROM {{ ref('inventtrans') }}            it
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid        = it.dataareaid
           AND ito.recid             = it.inventtransorigin
           AND ito.itemid            = it.itemid
           AND ito.referencecategory = 3
         INNER JOIN {{ ref('vendinvoicetrans') }}  vit
            ON vit.dataareaid        = ito.dataareaid
           AND vit.inventtransid     = ito.inventtransid
           AND vit.itemid            = ito.itemid
           AND vit.invoiceid         = it.invoiceid
         GROUP BY it.recid;
),
inventorytrans_factsales AS (
    SELECT DISTINCT
               it.recid         AS RecID_IT
             , MAX (cit.recid) AS RecID_CIT

          FROM {{ ref('inventtrans') }}            it
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid       = it.dataareaid
           AND ito.recid             = it.inventtransorigin
           AND ito.itemid            = it.itemid
           AND ito.referencecategory = 0
         INNER JOIN {{ ref('custinvoicetrans') }}  cit
            ON cit.dataareaid        = ito.dataareaid
           AND cit.inventtransid     = ito.inventtransid
           AND cit.itemid            = ito.itemid
           AND cit.invoiceid         = it.invoiceid
         GROUP BY it.recid;
),
inventorytrans_factfindim AS (
    SELECT it.recid                  AS RECID_IT
             , MAX (itp.defaultdimension) AS DefaultDimension

          FROM {{ ref('inventtrans') }}             it
         INNER JOIN {{ ref('inventtransposting') }} itp
            ON it.dataareaid        = itp.dataareaid
           AND it.inventtransorigin = itp.inventtransorigin
           AND it.voucher           = itp.voucher
           AND it.datefinancial     = itp.transdate
         GROUP BY it.recid;
),
inventorytrans_factstage AS (
    SELECT it.recid                                                                                         AS RecID
             , pl.recid                                                                                         AS RecID_PL
             , sl.recid                                                                                         AS RecID_SL
             , tp.RecID_VIT                                                                                      AS RecID_VIT
             , ts.RecID_CIT                                                                                      AS RecID_CIT
             , it.dataareaid                                                                                    AS LegalEntityID
             , CASE WHEN it.statusissue > 0 THEN 1 ELSE 2 END                                                    AS TransStatusTypeID
             , CASE WHEN it.statusissue > 0 THEN it.statusissue ELSE it.statusreceipt END                        AS TransStatusID
             , ito.referencecategory                                                                             AS TransSourceID
             , ito.recid                                                                                        AS RecID_ITO
             , it.itemid                                                                                         AS ItemID
             , id.inventbatchid                                                                                  AS TagID
             , id.inventsiteid                                                                                   AS SiteID
             , pl.vendaccount                                                                                    AS VendorID
             , id.inventlocationid                                                                               AS WarehouseID
             , id.wmslocationid                                                                                  AS WarehouseLocationID
             , CAST(pl.deliverydate AS DATE)                                                                     AS DeliveryDate_PL
             , CAST(sl.shippingdateconfirmed AS DATE)                                                            AS DeliveryDate_SL
             , id.configid                                                                                       AS ProductConfig
             , id.inventcolorid                                                                                  AS ProductLength
             , id.inventstyleid                                                                                  AS ProductColor
             , id.inventsizeid                                                                                   AS ProductWidth
             , it.costamountposted                                                                               AS PostedCost
             , it.costamountadjustment                                                                           AS PostedCostAdjustment
             , it.costamountphysical                                                                             AS PhysicalCost
             , CASE WHEN it.statusissue = 1
                      OR it.statusreceipt = 1
                    THEN it.costamountposted + it.costamountadjustment
                    WHEN it.statusissue = 2
                      OR it.statusreceipt = 2
                    THEN it.costamountphysical
                    ELSE 0 END                                                                                   AS OnHandCost
             , CASE WHEN it.statusissue IN ( 1, 2, 3 ) OR it.statusreceipt IN ( 1, 2, 3 ) THEN it.qty ELSE 0 END AS OnHandQuantity
             , it.qty                                                                                            AS TransQuantity
             , fd.DefaultDimension                                                                               AS DefaultDimension
             , sl.qtyordered                                                                                     AS OrderedQuantity_SL
             , pl.qtyordered                                                                                     AS OrderedQuantity_PL
             , it.dateclosed                                                                                     AS DateClosed
             , it.datefinancial                                                                                  AS DateFinancial
             , it.datephysical                                                                                   AS DatePhysical
             , it.dateinvent                                                                                     AS DateInventory
             , it.datestatus                                                                                     AS DateTransStatus
             , it.invoiceid                                                                                      AS InvoiceID
             , ISNULL (sl.purchorderformnum, sh.purchorderformnum)                                               AS CustomerPO
             , it.invoicereturned                                                                                AS InvoiceReturned
             , it.packingslipid                                                                                  AS PackingSlipID
             , it.packingslipreturned                                                                            AS PackingSlipReturned
             , it.voucher                                                                                        AS VoucherID
             , it.voucherphysical                                                                                AS PhysicalVoucherID
             , CASE WHEN it.statusissue IN ( 1, 2, 3 )
                      OR it.statusreceipt IN ( 1, 2, 3 )
                    THEN CASE WHEN NULLIF(mib.proddate, '1/1/1900') IS NOT NULL
                              THEN CASE WHEN DATEDIFF (d, mib.proddate, SYSDATETIME ()) = 0 THEN 1 ELSE
                                                                                                   DATEDIFF (
                                                                                                   d
                                                                                                   , mib.proddate
                                                                                                   , SYSDATETIME ()) END
                              WHEN COALESCE (
                                       NULLIF(it.datephysical, '1/1/1900')
                                     , NULLIF(it.datefinancial, '1/1/1900')
                                     , NULLIF(it.dateinvent, '1/1/1900')
                                     , NULLIF(it.datestatus, '1/1/1900')) IS NOT NULL
                              THEN CASE WHEN DATEDIFF (
                                                 d
                                               , COALESCE (
                                                     NULLIF(it.datephysical, '1/1/1900')
                                                   , NULLIF(it.datefinancial, '1/1/1900')
                                                   , NULLIF(it.dateinvent, '1/1/1900')
                                                   , NULLIF(it.datestatus, '1/1/1900'))
                                               , SYSDATETIME ()) = 0
                                        THEN 1
                                        ELSE
                                        DATEDIFF (
                                            d
                                          , COALESCE (
                                                NULLIF(it.datephysical, '1/1/1900')
                                              , NULLIF(it.datefinancial, '1/1/1900')
                                              , NULLIF(it.dateinvent, '1/1/1900')
                                              , NULLIF(it.datestatus, '1/1/1900'))
                                          , SYSDATETIME ()) END
                              ELSE NULL END
                    ELSE NULL END                                                                                AS DaysInInventory
             , CASE WHEN it.statusissue IN ( 1, 2, 3 ) OR it.statusreceipt IN ( 1, 2, 3 ) THEN 1 ELSE 0 END      AS OnHandTrans
             , id.inventstatusid                                                                                 AS INVENTSTATUSID
             , it.dateexpected

          FROM {{ ref('inventtrans') }}            it
          LEFT JOIN inventorytrans_factfindim               fd
            ON fd.RECID_IT           = it.recid
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.recid            = it.inventtransorigin
         INNER JOIN {{ ref('inventdim') }}         id
            ON id.dataareaid        = it.dataareaid
           AND id.inventdimid        = it.inventdimid
          LEFT JOIN  {{ ref('inventbatch') }}        ib
            ON ib.dataareaid        = it.dataareaid
           AND ib.inventbatchid      = id.inventbatchid
           AND ib.inventbatchid      <> ''
           AND ib.itemid             = it.itemid
          LEFT JOIN {{ ref('inventbatch') }}       mib
            ON mib.inventbatchid     = ib.cmamasterinventbatch
           AND mib.itemid            = ib.itemid
           AND mib.dataareaid       = ib.dataareaid
          LEFT JOIN {{ ref('purchline') }}         pl
            ON pl.dataareaid        = ito.dataareaid
           AND pl.inventtransid      = ito.inventtransid
           AND pl.itemid             = ito.itemid
           AND ito.referencecategory = 3
          LEFT JOIN {{ ref('salesline') }}         sl
            ON sl.dataareaid        = ito.dataareaid
           AND sl.inventtransid      = ito.inventtransid
           AND sl.itemid             = ito.itemid
           AND ito.referencecategory = 0
          LEFT JOIN {{ ref('salestable') }}       sh
            ON sh.dataareaid         = sl.dataareaid
           AND sh.salesid            = sl.salesid
          LEFT JOIN inventorytrans_factpurchase             tp
            ON tp.RecID_IT           = it.recid
          LEFT JOIN inventorytrans_factsales                ts
            ON ts.RecID_IT           = it.recid
         WHERE it.statusissue   <> 7
           AND it.statusreceipt <> 6;
),
inventorytrans_factinventory AS (
    SELECT ts.RecID                   AS RecID
             , ts.RecID_PL                AS RecID_PL
             , ts.RecID_SL                AS RecID_SL
             , ts.RecID_VIT               AS RecID_VIT
             , ts.RecID_CIT               AS RecID_CIT
             , dd1.DateKey                AS DeliveryDateKey_PL
             , dd2.DateKey                AS DeliveryDateKey_SL
             , ts.LegalEntityID           AS LegalEntityID
             , ts.TransStatusTypeID       AS TransStatusTypeID
             , ts.TransStatusID           AS TransStatusID
             , ts.TransSourceID           AS TransSourceID
             , ts.RecID_ITO               AS RecID_ITO
             , ts.ItemID                  AS ItemID
             , ts.TagID                   AS TagID
             , ts.SiteID                  AS SiteID
             , ts.WarehouseID             AS WarehouseID
             , ts.WarehouseLocationID     AS WarehouseLocationID
             , ts.DefaultDimension        AS DefaultDimension
             , ts.VendorID                AS VendorID
             , dp.ProductID               AS ProductID
             , ISNULL (dp.ProductKey, -1) AS ProductKey
             , ts.OrderedQuantity_SL      AS OrderedQuantity_SL
             , ts.OrderedQuantity_PL      AS OrderedQuantity_PL
             , ts.PostedCost              AS PostedCost
             , ts.PostedCostAdjustment    AS PostedCostAdjustment
             , ts.PhysicalCost            AS PhysicalCost
             , ts.OnHandCost              AS OnHandCost
             , ts.OnHandQuantity          AS OnHandQuantity
             , ts.TransQuantity           AS TransQuantity
             , ts.DateClosed              AS DateClosed
             , ts.DateFinancial           AS DateFinancial
             , ts.DatePhysical            AS DatePhysical
             , ts.DateInventory           AS DateInventory
             , ts.DateTransStatus         AS DateTransStatus
             , ts.CustomerPO              AS CustomerPO
             , ts.InvoiceID               AS InvoiceID
             , ts.InvoiceReturned         AS InvoiceReturned
             , ts.PackingSlipID           AS PackingSlipID
             , ts.PackingSlipReturned     AS PackingSlipReturned
             , ts.VoucherID               AS VoucherID
             , ts.PhysicalVoucherID       AS PhysicalVoucherID
             , dp.InventoryUOM            AS InventoryUnit
             , ts.DaysInInventory         AS DaysInInventory
             , ts.OnHandTrans             AS OnHandTrans
             , ts.INVENTSTATUSID          AS INVENTSTATUSID

          FROM inventorytrans_factstage               ts
         INNER JOIN {{ ref('legalentity_d') }} le
            ON le.LegalEntityID = ts.LegalEntityID
          LEFT JOIN {{ ref('product_d') }}     dp
            ON dp.LegalEntityID = ts.LegalEntityID
           AND dp.ItemID        = ts.ItemID
           AND dp.ProductLength = ts.ProductLength
           AND dp.ProductColor  = ts.ProductColor
           AND dp.ProductWidth  = ts.ProductWidth
           AND dp.ProductConfig = ts.ProductConfig
          LEFT JOIN {{ ref('date_d') }}        dd1
            ON dd1.Date         = ts.DeliveryDate_PL
          LEFT JOIN {{ ref('date_d') }}        dd2
            ON dd2.Date         = ts.DeliveryDate_SL;
),
inventorytrans_factdetailmain AS (
    SELECT le.LegalEntityKey                    AS LegalEntityKey
             , ab.AgingBucketKey                    AS AgingBucketKey
             , CASE WHEN fsl.SalesOrderLineKey <> -1
                    THEN t1.DeliveryDateKey_SL
                    WHEN dpl.PurchaseOrderLineKey <> -1
                    THEN t1.DeliveryDateKey_PL
                    ELSE CAST('19000101' AS INT)END AS DeliveryDateKey
             , fd1.FinancialKey                     AS FinancialKey
             , t1.ProductKey                        AS ProductKey
             , dt.TagKey                            AS TagKey
             , dts.InventoryTransStatusKey          AS InventoryTransStatusKey
             , iss.InventoryStatusKey               AS InventoryStatusKey
             , dits.InventorySourceKey              AS InventorySourceKey
             , dpl.PurchaseOrderLineKey             AS PurchaseOrderLineKey
             , pil.PurchaseInvoiceLineKey           AS PurchaseInvoiceLineKey
             , dv.VendorKey                         AS VendorKey
             , dw.WarehouseKey                      AS WarehouseKey
             , dwl.WarehouseLocationKey             AS WarehouseLocationKey
             , fsl.SalesOrderLineKey                AS SalesOrderLineKey
             , sil.SalesInvoiceLineKey              AS SalesInvoiceLineKey
             , dis.InventorySiteKey                 AS InventorySiteKey
             , il.LotKey                            AS LotKey
             , du.UOMKey                            AS InventoryUnitKey
             , vou.VoucherKey                       AS VoucherKey
             , vou1.VoucherKey                      AS VoucherPhysicalKey
             , t1.PhysicalCost
             , t1.PostedCost
             , t1.PostedCostAdjustment
             , t1.OnHandCost
             , t1.OnHandQuantity
             , t1.TransQuantity
             , t1.DateClosed
             , t1.DateFinancial
             , t1.DateInventory
             , t1.DatePhysical
             , t1.DateTransStatus
             , t1.DaysInInventory
             , t1.InvoiceID
             , t1.InvoiceReturned
             , t1.LegalEntityID
             , t1.PackingSlipID
             , t1.PackingSlipReturned
             , t1.OnHandTrans
             , 1                                    AS _SourceID
             , t1.RecID                             AS _RecID

          FROM inventorytrans_factinventory                    t1
         INNER JOIN {{ ref('legalentity_d') }}          le
            ON le.LegalEntityID               = t1.LegalEntityID
          LEFT JOIN {{ ref('inventorysource_d') }}      dits
            ON dits.InventorySourceID         = t1.TransSourceID
          LEFT JOIN {{ ref('inventory_trans_status_d') }} dts
            ON dts.InventoryTransStatusTypeID = t1.TransStatusTypeID
           AND dts.InventoryTransStatusID     = t1.TransStatusID
          LEFT JOIN {{ ref('agingbucket_d') }}          ab
            ON t1.DaysInInventory BETWEEN ab.AgeDaysBegin AND ab.AgeDaysEnd
          LEFT JOIN {{ ref('financial_d') }}            fd1
            ON fd1._RecID                     = t1.DefaultDimension
           AND fd1._SourceID                  = 1
          LEFT JOIN {{ ref('inventorysite_d') }}        dis
            ON dis.LegalEntityID              = t1.LegalEntityID
           AND dis.InventorySiteID            = t1.SiteID
          LEFT JOIN {{ ref('inventorystatus_d') }}      iss
            ON iss.LegalEntityID              = t1.LegalEntityID
           AND iss.InventoryStatusID          = t1.INVENTSTATUSID
          LEFT JOIN {{ ref('warehouse_d') }}            dw
            ON dw.LegalEntityID               = t1.LegalEntityID
           AND dw.WarehouseID                 = t1.WarehouseID
          LEFT JOIN {{ ref('warehouselocation_d') }}    dwl
            ON dwl.LegalEntityID              = t1.LegalEntityID
           AND dwl.WarehouseID                = t1.WarehouseID
           AND dwl.WarehouseLocation          = t1.WarehouseLocationID
          LEFT JOIN {{ ref('vendor_d') }}               dv
            ON dv.LegalEntityID               = t1.LegalEntityID
           AND dv.VendorAccount               = t1.VendorID
          LEFT JOIN {{ ref('lot_d') }}                  il
            ON il._RecID                      = t1.RecID_ITO
           AND il._SourceID                   = 1
          LEFT JOIN {{ ref('tag_d') }}                  dt
            ON dt.LegalEntityID               = t1.LegalEntityID
           AND dt.TagID                       = t1.TagID
           AND dt.ItemID                      = t1.ItemID
          LEFT JOIN {{ ref('uom_d') }}                  du
            ON du.UOM                         = t1.InventoryUnit
          LEFT JOIN {{ ref('salesorderline_d') }}       fsl
            ON fsl._RecID                     = t1.RecID_SL
           AND fsl._SourceID                  = 1
          LEFT JOIN {{ ref('purchaseorderline_d') }}    dpl
            ON dpl._RecID                     = t1.RecID_PL
           AND dpl._SourceID                  = 1
          LEFT JOIN {{ ref('salesinvoiceline_d') }}     sil
            ON sil._RecID2                    = t1.RecID_CIT
           AND sil._SourceID                  = 1
          LEFT JOIN {{ ref('purchaseinvoiceline_d') }}  pil
            ON pil._RecID2                    = t1.RecID_VIT
           AND pil._SourceID                  = 1
          LEFT JOIN {{ ref('voucher_d') }}              vou
            ON vou.LegalEntityID              = t1.LegalEntityID
           AND vou.VoucherID                  = t1.VoucherID
          LEFT JOIN {{ ref('voucher_d') }}              vou1
            ON vou1.LegalEntityID             = t1.LegalEntityID
           AND vou1.VoucherID                 = t1.PhysicalVoucherID;
)
SELECT ROW_NUMBER() OVER (ORDER BY td._RecID, td._SourceID) AS InventoryTransKey
         , td.InventoryTransStatusKey
         , td.AgingBucketKey
         , td.InventoryStatusKey
         , td.DeliveryDateKey
         , td.FinancialKey
         , td.InventorySourceKey
         , td.LegalEntityKey
         , td.LotKey
         , td.ProductKey
         , td.PurchaseInvoiceLineKey
         , td.PurchaseOrderLineKey
         , td.SalesInvoiceLineKey
         , td.SalesOrderLineKey
         , td.InventorySiteKey
         , td.TagKey
         , td.VendorKey
         , td.VoucherKey
         , td.VoucherPhysicalKey
         , td.WarehouseKey
         , td.WarehouseLocationKey
         , td.OnHandCost										 AS OnHandCost
         , td.OnHandQuantity                                     AS OnHandQuantity
         , td.OnHandQuantity * ISNULL(vuc.factor, 0)             AS OnHandQuantity_FT

         , td.OnHandQuantity * ISNULL(vuc2.factor, 0)            AS OnHandQuantity_LB
         , ROUND (td.OnHandQuantity * ISNULL(vuc3.factor, 0), 0) AS OnHandQuantity_PC
         , td.OnHandQuantity * ISNULL(vuc4.factor, 0)            AS OnHandQuantity_SQIN

         , td.PhysicalCost
         , td.PostedCost
         , td.PostedCostAdjustment
         , td.TransQuantity
         , td.TransQuantity * ISNULL(vuc.factor, 0)              AS TransQuantity_FT

         , td.TransQuantity * ISNULL(vuc2.factor, 0)                 AS TransQuantity_LB
         , ROUND (td.TransQuantity * ISNULL(vuc3.factor, 0), 0)  AS TransQuantity_PC
         , td.TransQuantity * ISNULL(vuc4.factor, 0)             AS TransQuantity_SQIN

         , td.DateClosed
         , td.DateFinancial
         , td.DateInventory
         , td.DatePhysical
         , td.DateTransStatus
         , td.DaysInInventory
         , td.InvoiceID
         , td.InvoiceReturned
         , td.PackingSlipID
         , td.PackingSlipReturned
         , td.OnHandTrans
         , td._SourceID
         , td._RecID
         , CURRENT_TIMESTAMP                                                            AS _CreatedDate
         , CURRENT_TIMESTAMP                                                            AS _ModifiedDate  

      FROM inventorytrans_factdetailmain              td
      LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc
        ON vuc.productkey      = td.ProductKey
       AND vuc.fromuomkey      = td.InventoryUnitKey
    -- AND vuc.touom           = 'FT'
       AND vuc.legalentitykey  = td.LegalEntityKey

      LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc2
        ON vuc2.productkey     = td.ProductKey
       AND vuc2.fromuomkey     = td.InventoryUnitKey
   --  AND vuc2.touom          = 'LB'
       AND vuc2.legalentitykey = td.LegalEntityKey
      LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc3
        ON vuc3.productkey     = td.ProductKey
       AND vuc3.fromuomkey     = td.InventoryUnitKey
   --  AND vuc3.touom          = 'PC'
       AND vuc3.legalentitykey = td.LegalEntityKey
      LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc4
        ON vuc4.productkey     = td.ProductKey
       AND vuc4.fromuomkey     = td.InventoryUnitKey
   --  AND vuc4.touom          = 'SQIN'
       AND vuc4.legalentitykey = td.LegalEntityKey
