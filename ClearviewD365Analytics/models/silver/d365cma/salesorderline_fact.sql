{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/salesorderline_fact/salesorderline_fact.py
-- Root method: SalesorderlineFact.salesorderline_factdetail [SalesOrderLine_FactDetail]
-- Inlined methods: SalesorderlineFact.salesorderline_factshipment [SalesOrderLine_FactShipment], SalesorderlineFact.salesorderline_factcost [SalesOrderLine_FactCost], SalesorderlineFact.salesorderline_factreserved [SalesOrderLine_FactReserved], SalesorderlineFact.salesorderline_factparentchildbom [SalesOrderLine_FactParentChildBOM], SalesorderlineFact.salesorderline_factsalesparentitem [SalesOrderLine_FactSalesParentItem], SalesorderlineFact.salesorderline_factparenttrans [SalesOrderLine_FactParentTrans], SalesorderlineFact.salesorderline_factparentitem [SalesOrderLine_FactParentItem], SalesorderlineFact.salesorderline_factstage [SalesOrderLine_FactStage], SalesorderlineFact.salesorderline_factcharge [SalesOrderLine_FactCharge], SalesorderlineFact.salesorderline_factexhangerate [SalesOrderLine_FactExhangeRate], SalesorderlineFact.salesorderline_factdetailmain [SalesOrderLine_FactDetailMain], SalesorderlineFact.salesorderline_factdetail1 [SalesOrderLine_FactDetail1], SalesorderlineFact.salesorderline_factdetail_base [SalesOrderLine_FactDetailBase], SalesorderlineFact.salesorderline_factdetail_cad [SalesOrderLine_FactDetailCAD], SalesorderlineFact.salesorderline_factdetail_mxp [SalesOrderLine_FactDetailMXP]
-- external_table_name: SalesOrderLine_FactDetail
-- schema_name: temp

WITH
salesorderline_factshipment AS (
    SELECT sl.recid              AS RECID
             , MAX(cpst.deliverydate) AS ShippedDate
             , SUM(CAST(cpst.qty AS numeric(32,6)) )          AS ShippedQuantity_SalesUOM
             , SUM(CAST(cpst.inventqty AS numeric(32,6)))    AS ShippedQuantity

          FROM {{ ref('salesline') }}                 sl
         INNER JOIN {{ ref('custpackingsliptrans') }} cpst
            ON cpst.dataareaid   = sl.dataareaid
           AND cpst.inventtransid = sl.inventtransid
           AND cpst.inventdimid   = sl.inventdimid
         GROUP BY sl.recid;
),
salesorderline_factcost AS (
    SELECT sl.recid                                     AS RECID
             , SUM(CASE WHEN (it.costamountposted + it.costamountadjustment) <> 0
                        THEN it.costamountposted + it.costamountadjustment
                        ELSE it.costamountphysical END * -1) AS Cost

          FROM {{ ref('salesline') }}              sl
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON sl.dataareaid       = ito.dataareaid
           AND sl.inventtransid     = ito.inventtransid
           AND sl.itemid            = ito.itemid
         INNER JOIN {{ ref('inventtrans') }}       it
            ON it.inventtransorigin = ito.recid
         GROUP BY sl.recid;
),
salesorderline_factreserved AS (
    SELECT (SUM(CAST(it.qty AS NUMERIC(32,6)))) * -1 AS PhysicalReservedQuantity
             , MAX(it.datestatus) AS ReservedDate
             , sl.recid          AS RecID_SL

          FROM {{ ref('salesline') }}                       sl
          INNER JOIN {{ ref('inventtransoriginsalesline') }} osl
            ON sl.dataareaid       = osl.saleslinedataareaid
           AND sl.inventtransid     = osl.saleslineinventtransid
         INNER JOIN {{ ref('inventtrans') }}                it
            ON it.inventtransorigin = osl.inventtransorigin
         WHERE it.statusreceipt = 0
           AND it.statusissue   = 4
         GROUP BY sl.recid;
),
salesorderline_factparentchildbom AS (
    SELECT *

          FROM (   SELECT bv.dataareaid                             AS ChildItemDataAreaID
                        , bv.bomid                                   AS BOMID
                        , bv.itemid                                  AS ChildItemID
                        , b.itemid                                   AS ParentItemID
                        , id.inventdimid                             AS INVENTDIMID
                        , id.configid                                AS CONFIGID
                        , id.inventcolorid                           AS INVENTCOLORID
                        , id.inventsizeid                            AS INVENTSIZEID
                        , id.inventstyleid                           AS INVENTSTYLEID
                        , id.inventsiteid                            AS INVENTSITEID
                        , ROW_NUMBER() OVER (PARTITION BY bv.dataareaid
                                                        , bv.bomid
                                                        , bv.itemid
                                                        , id.inventdimid
                                                        , id.configid
                                                        , id.inventcolorid
                                                        , id.inventsizeid
                                                        , id.inventstyleid
                                                        , id.inventsiteid
                                                 ORDER BY bv.bomid
                                                        , bv.itemid) AS RankVal
                     FROM {{ ref('bomversion') }}     bv
                     LEFT JOIN {{ ref('inventdim') }} id
                       ON id.dataareaid = bv.dataareaid
                      AND id.inventdimid = bv.inventdimid
                     LEFT JOIN {{ ref('bom') }}       b
                       ON b.dataareaid  = bv.dataareaid
                      AND b.bomid        = bv.bomid
                    WHERE bv.active = 1) a
         WHERE a.RankVal = 1;
),
salesorderline_factsalesparentitem AS (
    SELECT t.*

          FROM (   SELECT DISTINCT
                          sl.recid
                        , COALESCE(tb1.CONFIGID, tb2.CONFIGID, tb3.CONFIGID, tb4.CONFIGID)                     AS ConfigID
                        , COALESCE(tb1.INVENTCOLORID, tb2.INVENTCOLORID, tb3.INVENTCOLORID, tb4.INVENTCOLORID) AS INVENTCOLORID
                        , COALESCE(tb1.INVENTSIZEID, tb2.INVENTSIZEID, tb3.INVENTSIZEID, tb4.INVENTSIZEID)     AS INVENTSIZEID
                        , COALESCE(tb1.INVENTSTYLEID, tb2.INVENTSTYLEID, tb3.INVENTSTYLEID, tb4.INVENTSTYLEID) AS INVENTSTYLEID
                        , COALESCE(tb1.ParentItemID, tb2.ParentItemID, tb3.ParentItemID, tb4.ParentItemID)     AS ParentItemID
                     FROM {{ ref('salesline') }}                       sl
                     LEFT JOIN {{ ref('inventdim') }}                  id
                       ON id.inventdimid = sl.inventdimid
                    OUTER APPLY (   SELECT TOP 1
                                           tb.ParentItemID
                                         , tb.CONFIGID
                                         , tb.INVENTCOLORID
                                         , tb.INVENTSIZEID
                                         , tb.INVENTSTYLEID
                                         , tb.INVENTSITEID
                                      FROM salesorderline_factparentchildbom tb
                                     WHERE tb.ChildItemDataAreaID = sl.dataareaid
                                       AND tb.ChildItemID         = sl.itemid
                                       AND tb.CONFIGID            = id.configid
                                       AND tb.INVENTCOLORID       = id.inventcolorid
                                       AND tb.INVENTSIZEID        = id.inventsizeid
                                       AND tb.INVENTSTYLEID       = id.inventstyleid
                                       AND tb.INVENTSITEID        = id.inventsiteid
                                     ORDER BY tb.ParentItemID
                                            , tb.INVENTDIMID) tb1
                    OUTER APPLY (   SELECT TOP 1
                                           tb.ParentItemID
                                         , tb.CONFIGID
                                         , tb.INVENTCOLORID
                                         , tb.INVENTSIZEID
                                         , tb.INVENTSTYLEID
                                         , tb.INVENTSITEID
                                      FROM salesorderline_factparentchildbom tb
                                     WHERE tb.ChildItemDataAreaID = sl.dataareaid
                                       AND tb.ChildItemID         = sl.itemid
                                       AND tb.CONFIGID            = id.configid
                                       AND tb.INVENTCOLORID       = id.inventcolorid
                                       AND tb.INVENTSIZEID        = id.inventsizeid
                                       AND tb.INVENTSTYLEID       = id.inventstyleid
                                     ORDER BY tb.ParentItemID
                                            , tb.INVENTDIMID) tb2
                    OUTER APPLY (   SELECT TOP 1
                                           tb.ParentItemID
                                         , tb.CONFIGID
                                         , tb.INVENTCOLORID
                                         , tb.INVENTSIZEID
                                         , tb.INVENTSTYLEID
                                         , tb.INVENTSITEID
                                      FROM salesorderline_factparentchildbom tb
                                     WHERE tb.ChildItemDataAreaID = sl.dataareaid
                                       AND tb.ChildItemID         = sl.itemid
                                       AND tb.INVENTSITEID        = id.inventsiteid
                                     ORDER BY tb.ParentItemID
                                            , tb.INVENTDIMID) tb3
                    OUTER APPLY (   SELECT TOP 1
                                           tb.ParentItemID
                                         , tb.CONFIGID
                                         , tb.INVENTCOLORID
                                         , tb.INVENTSIZEID
                                         , tb.INVENTSTYLEID
                                         , tb.INVENTSITEID
                                      FROM salesorderline_factparentchildbom tb
                                     WHERE tb.ChildItemDataAreaID = sl.dataareaid
                                       AND tb.ChildItemID         = sl.itemid
                                     ORDER BY tb.ParentItemID
                                            , tb.INVENTDIMID) tb4
                    WHERE sl.salesstatus <= 1) t
         WHERE t.ParentItemID IS NOT NULL;
),
salesorderline_factparenttrans AS (
    SELECT DISTINCT
               sl.recid       AS RecID
             , it.dataareaid  AS DATAAREAID
             , ib.cmartsparent AS ParentTag
             , MAX(ib1.itemid) AS ParentITEMID

          FROM {{ ref('salesline') }}              sl
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON sl.dataareaid       = ito.dataareaid
           AND sl.inventtransid     = ito.inventtransid
           AND sl.itemid            = ito.itemid
         INNER JOIN {{ ref('inventtrans') }}       it
            ON it.inventtransorigin = ito.recid
         INNER JOIN {{ ref('inventdim') }}         id
            ON id.dataareaid       = it.dataareaid
           AND id.inventdimid       = it.inventdimid
         INNER JOIN  {{ ref('inventbatch') }}        ib
            ON ib.dataareaid       = id.dataareaid
           AND ib.inventbatchid     = id.inventbatchid
           AND ib.inventbatchid     <> ''
          LEFT JOIN  {{ ref('inventbatch') }}       ib1
            ON ib1.dataareaid      = ib.dataareaid
           AND ib1.inventbatchid    = ib.cmartsparent
         WHERE ib.cmartsparent <> ''
         GROUP BY sl.recid
                , it.dataareaid
                , ib.cmartsparent;
),
salesorderline_factparentitem AS (
    SELECT t.*

          FROM (   SELECT pt.RecID
                        , it.itemid AS ParentItemID
                        , id.inventcolorid
                        , id.inventstyleid
                        , id.inventsizeid
                        , id.configid
                        , ROW_NUMBER() OVER (PARTITION BY pt.RecID
    ORDER BY pt.RecID  )           AS RankVal
                     FROM salesorderline_factparenttrans         pt
                    INNER JOIN {{ ref('inventtrans') }} it
                       ON pt.DATAAREAID   = it.dataareaid
                      AND pt.ParentITEMID  = it.itemid
                    INNER JOIN {{ ref('inventdim') }}   id
                       ON id.dataareaid   = it.dataareaid
                      AND id.inventdimid   = it.inventdimid
                      AND id.inventbatchid = pt.ParentTag) t
         WHERE t.RankVal = 1;
),
salesorderline_factstage AS (
    SELECT sl.recid                                                                                                     AS RECID
             , sl.salescategory                                                                                              AS RecID_SC
             , al.recid                                                                                                     AS RecID_AL
             , sl.deliverypostaladdress                                                                                      AS DeliveryPostalAddress
             , sl.dataareaid                                                                                                AS LegalEntityID
             , sl.currencycode                                                                                               AS CurrencyID
             , CAST(sl.receiptdaterequested AS DATE)                                                                         AS ReceiptDateRequested
             , CAST(sl.receiptdateconfirmed AS DATE)                                                                         AS ReceiptDateConfirmed
             , CAST(sl.shippingdaterequested AS DATE)                                                                        AS ShipDateRequested
             , CAST(sl.shippingdateconfirmed AS DATE)                                                                        AS ShipDateConfirmed
             , CAST(sl.confirmeddlv AS DATE)                                                                                 AS ShipDate
             , CAST(tps.ShippedDate AS DATE)                                                                                 AS ShipDateActual
             , sh.dlvmode                                                                                                    AS DeliveryModeID
             , sh.dlvterm                                                                                                    AS DeliveryTermID
             , sh.payment                                                                                                    AS PaymentTermID
             , sl.cmapickingunit                                                                                      AS PickingUnit
             , sl.cmapriceuom                                                                                         AS PricingUnit
             , sl.salesunit                                                                                           AS SalesUOM
             , sh.taxgroup                                                                                                   AS TaxGroupID
             , CAST(CAST(sl.createddatetime AS datetime ) AT TIME ZONE 'UTC' AT TIME ZONE le.TimeZone AS DATE)                                 AS OrderDate
             , it.product                                                                                                    AS ProductID
             , sl.itemid                                                                                                     AS ItemID
             , COALESCE(tspi.ParentItemID, tpi.ParentItemID)                                                                 AS ParentItemID
             , COALESCE(tspi.INVENTCOLORID, tpi.INVENTCOLORID)                                                               AS ParentProductLength
             , COALESCE(tspi.INVENTSIZEID, tpi.INVENTSIZEID)                                                                 AS ParentProductWidth
             , COALESCE(tspi.INVENTSTYLEID, tpi.INVENTSTYLEID)                                                               AS ParentProductColor
             , COALESCE(tspi.ConfigID, tpi.ConfigID)                                                                         AS ParentProductConfig
             , id.inventcolorid                                                                                              AS ProductLength
             , id.inventsizeid                                                                                               AS ProductWidth
             , id.inventstyleid                                                                                              AS ProductColor
             , id.configid                                                                                                   AS ProductConfig
             , id.inventsiteid                                                                                               AS SiteID
             , sh.workersalesresponsible                                                                                     AS SalesPersonID
             , sh.salesid                                                                                                    AS SalesOrderID
             , ito.recid                                                                                                    AS RecID_ITO
             , id.inventlocationid                                                                                           AS WarehouseID
             , sh.custaccount                                                                                                AS CustomerAccount
             , sh.invoiceaccount                                                                                             AS InvoiceAccount
             , sl.defaultdimension                                                                                           AS DefaultDimension
             , sh.documentstatus                                                                                             AS DocumentStatusID
             , sl.salesstatus                                                                                                AS SalesLineStatusID
             , sh.salesstatus                                                                                                AS SalesStatusID
             , sl.salestype                                                                                                  AS SalesTypeID
             , sl.priceunit                                                                                                  AS PriceUnit
             , tic.Cost                                                                                                      AS Cost
             , sl.salesprice                                                                                                 AS BaseUnitPrice_TransCur
             , sl.cmatotalprice                                                                                              AS TotalUnitPrice_TransCur
             , im.unitid                                                                                              AS InventoryUnit
             , sl.salesprice * sl.salesqty / ISNULL(NULLIF(sl.priceunit, 0), 1)                                              AS BaseAmount_TransCur
             , sl.lineamount                                                                                                 AS NetAmount_TransCur
             , sl.cmatotalamount                                                                                             AS TotalAmount_TransCur
             , sl.salesqty                                                                                                   AS OrderedQuantity_SalesUOM
             , sl.qtyordered                                                                                                 AS OrderedQuantity
             , sl.salesprice * ISNULL(tps.ShippedQuantity_SalesUOM, 0) / ISNULL(NULLIF(sl.priceunit, 0), 1)                  AS ShippedAmount_TransCur
             , ISNULL(tps.ShippedQuantity_SalesUOM, 0)                                                                       AS ShippedQuantity_SalesUOM
             , ISNULL(tps.ShippedQuantity, 0)                                                                                AS ShippedQuantity
             , CASE WHEN sl.salesstatus IN ( 1, 2 )
                    THEN CASE WHEN sl.cmatotalprice <> 0
                               AND sl.salesprice <> 0
                              THEN ISNULL(sl.cmatotalprice, sl.salesprice) * sl.remainsalesphysical
                                   / ISNULL(NULLIF(sl.priceunit, 0), 1)
                              ELSE sl.remainsalesphysical * (sl.lineamount / ISNULL(NULLIF(sl.salesqty, 0), 1)) END
                    ELSE NULL END                                                                                            AS RemainingAmount_TransCur
             , CASE WHEN sl.salesstatus IN ( 1, 2 ) THEN sl.remainsalesphysical ELSE NULL END                                AS RemainingQuantity_SalesUOM
             , CASE WHEN sl.salesstatus IN ( 1, 2 ) THEN sl.remaininventphysical ELSE NULL END                               AS RemainingQuantity
             , sl.returnstatus                                                                                               AS ReturnStatus
             , sh.returnreasoncodeid                                                                                         AS ReturnReasonID
             , sh.workersalestaker                                                                                           AS SalesTaker
             , CAST(CASE WHEN sl.salestype = 4 THEN 1 ELSE 0 END AS SMALLINT)                                                AS ReturnLineCount
             , CAST(CASE WHEN sl.salestype = 3 AND CAST(tps.ShippedDate AS DATE) > '1/1/1900' THEN 1 ELSE 0 END AS SMALLINT) AS ShippedLineCount
             , CAST(1 AS SMALLINT)                                                                                           AS OrderLineCount
             , CASE WHEN sl.salesstatus NOT BETWEEN 1 AND 3
                    THEN NULL
                    WHEN sl.salestype NOT IN ( 3, 4 ) 
                    THEN NULL
                    WHEN sl.salestype = 4 
                    THEN 7
                    WHEN (tps.ShippedDate IS NULL OR CAST(tps.ShippedDate AS DATE) <= '1/1/1900')
                     AND sl.salesstatus IN ( 2, 3 ) 
                    THEN NULL
                    WHEN (sl.confirmeddlv IS NULL OR CAST(sl.confirmeddlv AS DATE) <= '1/1/1900')
                     AND sl.salesstatus = 1 
                    THEN 6 
                    WHEN (sl.confirmeddlv IS NULL OR CAST(sl.confirmeddlv AS DATE) <= '1/1/1900')
                     AND sl.salesstatus <> 1 
                    THEN 5 
                    WHEN sl.salesstatus = 1 
                     AND CAST(COALESCE(NULLIF(sl.confirmeddlv, '1/1/1900'), sl.shippingdaterequested) AS DATE) >= CAST(SYSDATETIME() AS DATE)
                    THEN 1 
                    WHEN sl.salesstatus = 1 
                     AND CAST(COALESCE(NULLIF(sl.confirmeddlv, '1/1/1900'), sl.shippingdaterequested) AS DATE) < CAST(SYSDATETIME() AS DATE)
                    THEN 2 
                    WHEN sl.salesstatus IN ( 2, 3 ) 
                     AND CAST(COALESCE(NULLIF(sl.confirmeddlv, '1/1/1900'), sl.shippingdaterequested) AS DATE) >= CAST(tps.ShippedDate AS DATE)
                    THEN 4 
                    WHEN sl.salesstatus IN ( 2, 3 ) 
                     AND CAST(COALESCE(NULLIF(sl.confirmeddlv, '1/1/1900'), sl.shippingdaterequested) AS DATE) < CAST(tps.ShippedDate AS DATE)
                    THEN 3 
                    ELSE NULL END                                                                                            AS OnTimeShipStatusID
             , CASE WHEN sl.purchorderformnum = '' THEN sh.purchorderformnum ELSE sl.purchorderformnum END                   AS CustomerPO
             , sl.customerref                                                                                                AS CustomerReference
             , tr.PhysicalReservedQuantity                                                                                   AS PhysicalReservedQuantity
             , sh.returnitemnum                                                                                              AS ReturnItemID
             , CASE WHEN sl.inventreftype = 3 THEN sl.inventrefid ELSE NULL END                                              AS ProductionOrder
             , CAST(tr.ReservedDate AS DATE)                                                                                 AS ReservedDate
             , sl.modifieddatetime                                                                                          AS _SourceDate
             , sl.createdby                                                                                                 AS CreatedByUserID

          FROM {{ ref('salesline') }}              sl
         INNER JOIN {{ ref('salestable') }}        sh
            ON sh.dataareaid    = sl.dataareaid
           AND sh.salesid        = sl.salesid
           AND sh.salesstatus    <> 4 
         INNER JOIN silver.cma_LegalEntity       le
            ON le.LegalEntityID  = sl.dataareaid
          LEFT JOIN {{ ref('inventtable') }}       it
            ON it.dataareaid    = sl.dataareaid
           AND it.itemid         = sl.itemid
          LEFT JOIN {{ ref('inventtablemodule') }} im
            ON im.dataareaid    = it.dataareaid
           AND im.itemid         = it.itemid
           AND im.moduletype     = 0
          LEFT JOIN {{ ref('inventdim') }}         id
            ON id.dataareaid    = sl.dataareaid
           AND id.inventdimid    = sl.inventdimid
          LEFT JOIN salesorderline_factshipment             tps
            ON tps.RECID         = sl.recid
          LEFT JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid   = sl.dataareaid
           AND ito.inventtransid = sl.inventtransid
          LEFT JOIN salesorderline_factcost                 tic
            ON tic.RECID         = sl.recid
          LEFT JOIN {{ ref('agreementline') }}     al
            ON al.recid         = sl.matchingagreementline
          LEFT JOIN salesorderline_factreserved             tr
            ON tr.RecID_SL       = sl.recid
          LEFT JOIN salesorderline_factsalesparentitem      tspi
            ON tspi.RECID       = sl.recid
          LEFT JOIN salesorderline_factparentitem           tpi
            ON tpi.RecID        = sl.recid
         WHERE sl.salesstatus <> 4;
),
salesorderline_factcharge AS (
    SELECT sol.SalesOrderLineKey               AS SalesOrderLineKey
             , SUM(crg.IncludedCharge)             AS IncludedCharge
             , SUM(crg.IncludedCharge_TransCur)    AS IncludedCharge_TransCur
             , SUM(crg.AdditionalCharge)           AS AdditionalCharge
             , SUM(crg.AdditionalCharge_TransCur)  AS AdditionalCharge_TransCur
             , SUM(crg.NonBillableCharge)          AS NonBillableCharge
             , SUM(crg.NonBillableCharge_TransCur) AS NonBillableCharge_TransCur

          FROM silver.cma_SalesOrderLine                 sol
         INNER JOIN silver.cma_SalesOrderLineCharge_Fact crg
            ON crg.SalesOrderLineKey = sol.SalesOrderLineKey

         GROUP BY sol.SalesOrderLineKey;
),
salesorderline_factexhangerate AS (
    SELECT t.*

          FROM (   SELECT ts.RECID           AS RecID_SL
                        , cij.exchrate / 100 AS ExchangeRate
                        , ROW_NUMBER() OVER (PARTITION BY sl.recid
    ORDER BY cij.invoicedate DESC)           AS Rank_val
                     FROM salesorderline_factstage                    ts
                    INNER JOIN {{ ref('salesline') }}        sl
                       ON sl.recid               = ts.RECID
                     LEFT JOIN {{ ref('custinvoicetrans') }} cit
                       ON cit.dataareaid         = sl.dataareaid
                      AND cit.inventtransid       = sl.inventtransid
                      AND cit.itemid              = sl.itemid
                    INNER JOIN {{ ref('custinvoicejour') }}  cij
                       ON cij.dataareaid         = cit.dataareaid
                      AND cij.salesid             = cit.salesid
                      AND cij.invoiceid           = cit.invoiceid
                      AND cij.invoicedate         = cit.invoicedate
                      AND cij.numbersequencegroup = cit.numbersequencegroup
                      AND (cij.recid             = cit.parentrecid OR cij.salestype <> 0)
                    WHERE ts.SalesLineStatusID = 3) AS t
         WHERE t.Rank_val = 1;
),
salesorderline_factdetailmain AS (
    SELECT DISTINCT sod.SalesOrderLineKey                                                       AS SalesOrderLineKey
             , so.SalesOrderKey                                                            AS SalesOrderKey
             , sal.SalesAgreementLineKey                                                   AS SalesAgreementLineKey
             , le.LegalEntityKey                                                           AS LegalEntityKey
             , da.AddressKey                                                               AS DeliveryAddressKey
             , ds.DocumentStatusKey                                                        AS DocumentStatusKey
             , dc1.CustomerKey                                                             AS CustomerKey
             , cc.CurrencyKey                                                              AS CurrencyKey
             , dm.DeliveryModeKey                                                          AS DeliveryModeKey
             , dt.DeliveryTermKey                                                          AS DeliveryTermKey
             , fd1.FinancialKey                                                            AS FinancialKey
             , dc2.CustomerKey                                                             AS InvoiceCustomerKey
             , it.lotkey                                                                   AS LotKey
             , pyt.PaymentTermKey                                                          AS PaymentTermKey
             , pu.UOMKey                                                                   AS PricingUOMKey
             , pku.UOMKey                                                                  AS PickingUOMKey
             , su.UOMKey                                                                   AS SalesUOMKey
             , dd.DateKey                                                                  AS OrderDateKey
             , dp1.ProductKey                                                              AS ParentProductKey
             , ISNULL(dp.ProductKey, -1)                                                   AS ProductKey
             , prod.ProductionKey                                                          AS ProductionKey
             , dd5.DateKey                                                                 AS ReceiptDateRequestedKey
             , dd2.DateKey                                                                 AS ReceiptDateConfirmedKey
             , drr.ReturnReasonKey                                                         AS ReturnReasonkey
             , drs.ReturnStatusKey                                                         AS ReturnStatusKey
             , dd8.DateKey                                                                 AS ReservedDateKey
             , dsc.SalesCategoryKey                                                        AS SalesCategoryKey
             , sp.SalesPersonKey                                                           AS SalesPersonKey
             , ps.SalesStatusKey                                                           AS SalesStatusKey
             , ps1.SalesStatusKey                                                          AS SalesLineStatusKey
             , de2.EmployeeKey                                                             AS SalesTakerKey
             , pt.SalesTypeKey                                                             AS SalesTypeKey
             , ui.UserInfoKey                                                              AS UserInfoKey
             , ts.ReturnLineCount                                                          AS ReturnLineCount
             , ts.ShippedLineCount                                                         AS ShippedLineCount
             , ts.OrderLineCount                                                           AS OrderLineCount
             , CASE WHEN ts.OnTimeShipStatusID IN ( 1, 2 )
                    THEN DATEDIFF(DAY, ts.ShipDate, CAST(SYSDATETIME() AS DATE))
                    WHEN ts.OnTimeShipStatusID IN ( 3, 4 )
                    THEN DATEDIFF(DAY, ts.ShipDate, ts.ShipDateActual)
                    ELSE NULL END                                                          AS DaysLateTillDue
             , ot.OnTimeShipStatusKey                                                      AS OnTimeShipStatusKey
             , dd4.DateKey                                                                 AS ShipDateActualKey
             , dd3.DateKey                                                                 AS ShipDateConfirmedKey
             , dd6.DateKey                                                                 AS ShipDateRequestedKey
             , dd1.DateKey                                                                 AS ShipDateDueKey
             , dw.WarehouseKey                                                             AS WarehouseKey
             , dis.InventorySiteKey                                                        AS InventorySiteKey
             , tg.TaxGroupKey                                                              AS TaxGroupKey
             , ts.BaseAmount_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1)      AS BaseAmount
             , ts.BaseAmount_TransCur                                                      AS BaseAmount_TransCur
             , ts.Cost                                                                     AS Cost
             , ts.BaseUnitPrice_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1)   AS BaseUnitPrice
             , ts.BaseUnitPrice_TransCur                                                   AS BaseUnitPrice_TransCur
             , ts.TotalUnitPrice_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1)  AS TotalUnitPrice
             , ts.TotalUnitPrice_TransCur                                                  AS TotalUnitPrice_TransCur
             , ts.PriceUnit                                                                AS PriceUnit
             , ts.SalesStatusID
             , le.AccountingCurrencyID                                                     AS AccountingCurrencyID
             , le.TransExchangeRateType                                                    AS TransExchangeRateType
             , ca.IncludedCharge                                                           AS IncludedCharge
             , ca.IncludedCharge_TransCur                                                  AS IncludedCharge_TransCur
             , ca.AdditionalCharge                                                         AS AdditionalCharge
             , ca.AdditionalCharge_TransCur                                                AS AdditionalCharge_TransCur
             , ISNULL((ca.IncludedCharge + ca.AdditionalCharge), 0)                        AS CustomerCharge
             , ISNULL((ca.IncludedCharge_TransCur + ca.AdditionalCharge_TransCur), 0)      AS CustomerCharge_TransCur
             , ca.NonBillableCharge                                                        AS NonBillableCharge
             , ca.NonBillableCharge_TransCur                                               AS NonBillableCharge_TransCur
             , CASE WHEN ts.ReturnLineCount = 0
                    THEN (((CASE WHEN ts.BaseUnitPrice_TransCur = 0
                                 THEN CASE WHEN ts.TotalAmount_TransCur = 0
                                           THEN 0
                                           ELSE
             ( (ts.NetAmount_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1)
                / ISNULL(NULLIF(ts.OrderedQuantity_SalesUOM, 0), 1))) END
                                 ELSE ts.BaseUnitPrice_TransCur / (ISNULL(NULLIF(ts.PriceUnit, 0), 1)) END)
                           * (CASE WHEN ts.OrderedQuantity_SalesUOM = 0 THEN 1 ELSE ts.OrderedQuantity_SalesUOM END))
                          - (ts.NetAmount_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1)))
                    ELSE 0 END                                                             AS DiscountAmount
             , CASE WHEN ts.ReturnLineCount = 0
                    THEN (((CASE WHEN ts.BaseUnitPrice_TransCur = 0
                                 THEN CASE WHEN ts.TotalAmount_TransCur = 0
                                           THEN 0
                                           ELSE
             ( (ts.NetAmount_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1)
                / ISNULL(NULLIF(ts.OrderedQuantity_SalesUOM, 0), 1))) END
                                 ELSE ts.BaseUnitPrice_TransCur / (ISNULL(NULLIF(ts.PriceUnit, 0), 1)) END)
                           * (CASE WHEN ts.OrderedQuantity_SalesUOM = 0 THEN 1 ELSE ts.OrderedQuantity_SalesUOM END))
                          - ts.NetAmount_TransCur)
                    ELSE 0 END                                                             AS DiscountAmount_TransCur
             , ts.NetAmount_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1)       AS NetAmount
             , ts.NetAmount_TransCur                                                       AS NetAmount_TransCur
             , ts.OrderedQuantity_SalesUOM                                                 AS OrderedQuantity_SalesUOM
             , ts.OrderedQuantity                                                          AS OrderedQuantity
             , ts.ShippedAmount_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1)   AS ShippedAmount
             , ts.ShippedAmount_TransCur                                                   AS ShippedAmount_TransCur
             , ts.ShippedQuantity_SalesUOM                                                 AS ShippedQuantity_SalesUOM
             , ts.ShippedQuantity                                                          AS ShippedQuantity
             , ts.RemainingAmount_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1) AS RemainingAmount
             , ts.RemainingAmount_TransCur                                                 AS RemainingAmount_TransCur
             , ts.RemainingQuantity_SalesUOM                                               AS RemainingQuantity_SalesUOM
             , ts.RemainingQuantity                                                        AS RemainingQuantity
             , ts.PhysicalReservedQuantity                                                 AS PhysicalReservedQuantity
             , ts.PhysicalReservedQuantity * ISNULL(vuc.factor, 0)                         AS PhysicalReservedQuantity_SalesUOM
             , ts.SalesUOM                                                                 AS FromUOM
             , ts.SalesLineStatusID                                                        AS SalesLineStatusID
             , ts.SalesTypeID                                                              AS SalesTypeID
             , ts.LegalEntityID
             , ts.CustomerPO                                                               AS CustomerPO
             , ts.CustomerReference                                                        AS CustomerReference
             , ts.ReturnItemID                                                             AS ReturnItemID
             , ts._SourceDate                                                              AS _SourceDate
             , 1                                                                           AS _SourceID
             , ts.RECID                                                                    AS _RecID

          FROM salesorderline_factstage                      ts
         INNER JOIN silver.cma_SalesOrderLine     sod
            ON sod._RecID            = ts.RECID
           AND sod._SourceID         = 1
         INNER JOIN silver.cma_LegalEntity        le
            ON le.LegalEntityID      = ts.LegalEntityID
          LEFT JOIN silver.cma_SalesAgreementLine sal
            ON sal._RecID            = ts.RecID_AL
           AND sal._SourceID         = 1
          LEFT JOIN silver.cma_Address            da
            ON da._RecID             = ts.DeliveryPostalAddress
           AND da._SourceID          = 1
          LEFT JOIN silver.cma_Product            dp
            ON dp.LegalEntityID      = ts.LegalEntityID
           AND dp.ItemID             = ts.ItemID
              AND dp.ProductWidth = ts.ProductWidth
           AND dp.ProductLength = ts.ProductLength
           AND dp.ProductColor = ts.ProductColor
           AND dp.ProductConfig = ts.ProductConfig
          LEFT JOIN silver.cma_Product            dp1
            ON dp1.LegalEntityID     = ts.LegalEntityID
           AND dp1.ItemID            = ts.ParentItemID
            AND dp1.ProductWidth = ts.ParentProductWidth
           AND dp1.ProductLength = ts.ParentProductLength
           AND dp1.ProductColor = ts.ParentProductColor
           AND dp1.ProductConfig = ts.ParentProductConfig
          LEFT JOIN silver.cma_Date               dd
            ON dd.Date               = ts.OrderDate
          LEFT JOIN silver.cma_Date               dd1
            ON dd1.Date              = ts.ShipDate
          LEFT JOIN silver.cma_Date               dd2
            ON dd2.Date              = ts.ReceiptDateConfirmed
          LEFT JOIN silver.cma_Date               dd3
            ON dd3.Date              = ts.ShipDateConfirmed
          LEFT JOIN silver.cma_Date               dd4
            ON dd4.Date              = ts.ShipDateActual
          LEFT JOIN silver.cma_Date               dd5
            ON dd5.Date              = ts.ReceiptDateRequested
          LEFT JOIN silver.cma_Date               dd6
            ON dd6.Date              = ts.ShipDateRequested
          LEFT JOIN salesorderline_factexhangerate           er
            ON er.RecID_SL           = ts.RECID
          LEFT JOIN silver.cma_Date               dd7
            ON dd7.Date              = CAST(GETDATE() AS DATE)
          LEFT JOIN silver.cma_Date               dd8
            ON dd8.Date              = ts.ReservedDate
          LEFT JOIN silver.cma_ExchangeRate_Fact  ex
            ON ex.ExchangeDateKey    = dd7.DateKey
           AND ex.FromCurrencyID     = ts.CurrencyID
           AND ex.ToCurrencyID       = le.AccountingCurrencyID
           AND ex.ExchangeRateType   = le.TransExchangeRateType
          LEFT JOIN silver.cma_Customer           dc1
            ON dc1.LegalEntityID     = ts.LegalEntityID
           AND dc1.CustomerAccount   = ts.CustomerAccount
          LEFT JOIN silver.cma_Customer           dc2
            ON dc2.LegalEntityID     = ts.LegalEntityID
           AND dc2.CustomerAccount   = ts.InvoiceAccount
          LEFT JOIN silver.cma_InventorySite      dis
            ON dis.LegalEntityID     = ts.LegalEntityID
           AND dis.InventorySiteID   = ts.SiteID
          LEFT JOIN silver.cma_Financial          fd1
            ON fd1._RecID            = ts.DefaultDimension
           AND fd1._SourceID         = 1
          LEFT JOIN silver.cma_Warehouse          dw
            ON dw.LegalEntityID      = ts.LegalEntityID
           AND dw.WarehouseID        = ts.WarehouseID
          LEFT JOIN silver.cma_SalesPerson        sp
            ON sp._RecID             = ts.SalesPersonID
           AND sp._SourceID          = 1
          LEFT JOIN silver.cma_Lot                it
            ON it._recid             = ts.RecID_ITO
           AND it._sourceid          = 1
          LEFT JOIN silver.cma_DocumentStatus     ds
            ON ds.DocumentStatusID   = ts.DocumentStatusID
          LEFT JOIN silver.cma_SalesStatus        ps
            ON ps.SalesStatusID      = ts.SalesStatusID
          LEFT JOIN silver.cma_SalesStatus        ps1
            ON ps1.SalesStatusID     = ts.SalesLineStatusID
          LEFT JOIN silver.cma_SalesType          pt
            ON pt.SalesTypeID        = ts.SalesTypeID
          LEFT JOIN silver.cma_OnTimeShipStatus   ot
            ON ot.OnTimeShipStatusID = ts.OnTimeShipStatusID
          LEFT JOIN silver.cma_Currency           cc
            ON cc.CurrencyID         = ts.CurrencyID
          LEFT JOIN silver.cma_DeliveryMode       dm
            ON dm.LegalEntityID      = ts.LegalEntityID
           AND dm.DeliveryModeID     = ts.DeliveryModeID
          LEFT JOIN silver.cma_DeliveryTerm       dt
            ON dt.LegalEntityID      = ts.LegalEntityID
           AND dt.DeliveryTermID     = ts.DeliveryTermID
          LEFT JOIN silver.cma_PaymentTerm        pyt
            ON pyt.LegalEntityID     = ts.LegalEntityID
           AND pyt.PaymentTermID     = ts.PaymentTermID
          LEFT JOIN silver.cma_UOM                pu
            ON pu.UOM                = ts.PricingUnit
          LEFT JOIN silver.cma_UOM                pku
            ON pku.UOM               = ts.PickingUnit
          LEFT JOIN silver.cma_UOM                su
            ON su.UOM                = ts.SalesUOM
          LEFT JOIN silver.cma_TaxGroup           tg
            ON tg.LegalEntityID      = ts.LegalEntityID
           AND tg.TaxGroupID         = ts.TaxGroupID
          LEFT JOIN salesorderline_factcharge                ca
            ON ca.SalesOrderLineKey  = sod.SalesOrderLineKey
          LEFT JOIN silver.cma_ReturnStatus       drs
            ON drs.ReturnStatusID    = ts.ReturnStatus
          LEFT JOIN silver.cma_ReturnReason       drr
            ON drr.LegalEntityID     = ts.LegalEntityID
           AND drr.ReturnReasonID    = ts.ReturnReasonID
          LEFT JOIN silver.cma_Employee           de2
            ON de2._RecID            = ts.SalesTaker
          LEFT JOIN silver.cma_SalesCategory      dsc
            ON dsc._RecID            = ts.RecID_SC
           AND dsc._SourceID         = 1
          LEFT JOIN silver.cma_SalesOrder         so
            ON so.LegalEntityID      = ts.LegalEntityID
           AND so.SalesOrderID       = ts.SalesOrderID
          LEFT JOIN silver.cma_Production         prod
            ON prod.LegalEntityID    = ts.LegalEntityID
           AND prod.ProductionID     = ts.ProductionOrder
          LEFT JOIN {{ ref('vwuomconversion') }}    vuc
            ON vuc.legalentitykey    = le.LegalEntityKey
           AND vuc.productkey        = ISNULL(dp.ProductKey, -1)
           AND vuc.fromuom           = ts.InventoryUnit
           AND vuc.touom             = ts.SalesUOM
          LEFT JOIN silver.cma_UserInfo           ui
            ON ui.UserName           = ts.CreatedByUserID;
),
salesorderline_factdetail1 AS (
    SELECT DISTINCT td.SalesOrderLineKey                                                                                          AS SalesOrderLineKey
             , td.SalesOrderKey                                                                                              AS SalesOrderKey
             , td.SalesAgreementLineKey                                                                                      AS SalesAgreementLineKey
             , td.LegalEntityKey                                                                                             AS LegalEntityKey
             , td.DeliveryAddressKey                                                                                         AS DeliveryAddressKey
             , td.DocumentStatusKey                                                                                          AS DocumentStatusKey
             , td.CustomerKey                                                                                                AS CustomerKey
             , td.CurrencyKey                                                                                                AS CurrencyKey
             , td.DeliveryModeKey                                                                                            AS DeliveryModeKey
             , td.DeliveryTermKey                                                                                            AS DeliveryTermKey
             , td.FinancialKey                                                                                               AS FinancialKey
             , td.InvoiceCustomerKey                                                                                         AS InvoiceCustomerKey
             , td.LotKey                                                                                                     AS LotKey
             , td.PaymentTermKey                                                                                             AS PaymentTermKey
             , td.PricingUOMKey                                                                                              AS PricingUOMKey
             , td.PickingUOMKey                                                                                              AS PickingUOMKey
             , td.ProductionKey                                                                                              AS ProductionKey
             , td.SalesUOMKey                                                                                                AS SalesUOMKey
             , td.OrderDateKey                                                                                               AS OrderDateKey
             , td.OnTimeShipStatusKey                                                                                        AS OnTimeShipStatusKey
             , td.ParentProductKey                                                                                           AS ParentProductKey
             , td.ProductKey                                                                                                 AS ProductKey
             , td.ReceiptDateRequestedKey                                                                                    AS ReceiptDateRequestedKey
             , td.ReceiptDateConfirmedKey                                                                                    AS ReceiptDateConfirmedKey
             , td.SalesCategoryKey                                                                                           AS SalesCategoryKey
             , td.ReturnReasonkey                                                                                            AS ReturnReasonkey
             , td.ReturnStatusKey                                                                                            AS ReturnStatusKey
             , td.ReservedDateKey                                                                                            AS ReservedDateKey
             , td.SalesPersonKey                                                                                             AS SalesPersonKey
             , td.SalesStatusKey                                                                                             AS SalesStatusKey
             , td.SalesLineStatusKey                                                                                         AS SalesLineStatusKey
             , td.SalesTakerKey                                                                                              AS SalesTakerKey
             , td.SalesTypeKey                                                                                               AS SalesTypeKey
             , td.ShipDateActualKey                                                                                          AS ShipDateActualKey
             , td.ShipDateConfirmedKey                                                                                       AS ShipDateConfirmedKey
             , td.ShipDateRequestedKey                                                                                       AS ShipDateRequestedKey
             , td.ShipDateDueKey                                                                                             AS ShipDateDueKey
             , td.UserInfoKey                                                                                                AS UserInfoKey
             , td.SalesStatusID                                                                                              AS SalesStatusID
             , td.SalesTypeID                                                                                                AS SalesTypeID
             , CASE WHEN td.SalesTypeID <> 3 THEN NULL ELSE CASE WHEN td.SalesLineStatusID IN ( 1, 2 ) THEN 1 ELSE 0 END END AS OpenLineCount
             , td.ReturnLineCount                                                                                            AS ReturnLineCount
             , td.Cost                                                                                                       AS Cost
             , td.OrderLineCount                                                                                             AS OrderLineCount
             , td.DaysLateTillDue                                                                                            AS DaysLateTillDue
             , td.WarehouseKey                                                                                               AS WarehouseKey
             , td.InventorySiteKey                                                                                           AS InventorySiteKey
             , td.TaxGroupKey                                                                                                AS TaxGroupKey
             , td.BaseAmount                                                                                                 AS BaseAmount
             , td.BaseAmount_TransCur                                                                                        AS BaseAmount_TransCur
             , td.BaseUnitPrice                                                                                              AS BaseUnitPrice
             , td.BaseUnitPrice_TransCur                                                                                     AS BaseUnitPrice_TransCur
             , td.TotalUnitPrice                                                                                             AS TotalUnitPrice
             , td.TotalUnitPrice_TransCur                                                                                    AS TotalUnitPrice_TransCur
             , td.PriceUnit                                                                                                  AS PriceUnit
             , td.AccountingCurrencyID                                                                                       AS AccountingCurrencyID
             , td.TransExchangeRateType                                                                                      AS TransExchangeRateType
             , td.IncludedCharge                                                                                             AS IncludedCharge
             , td.IncludedCharge_TransCur                                                                                    AS IncludedCharge_TransCur
             , td.AdditionalCharge                                                                                           AS AdditionalCharge
             , td.AdditionalCharge_TransCur                                                                                  AS AdditionalCharge_TransCur
             , td.CustomerCharge                                                                                             AS CustomerCharge
             , td.CustomerCharge_TransCur                                                                                    AS CustomerCharge_TransCur
             , td.NonBillableCharge                                                                                          AS NonBillableCharge
             , td.NonBillableCharge_TransCur                                                                                 AS NonBillableCharge_TransCur
             , CASE WHEN td.DiscountAmount < 0 THEN td.DiscountAmount * -1 ELSE td.DiscountAmount END                        AS DiscountAmount
             , CASE WHEN td.DiscountAmount_TransCur < 0 THEN td.DiscountAmount_TransCur * -1 ELSE
                                                                                             td.DiscountAmount_TransCur END  AS DiscountAmount_TransCur
             , td.NetAmount                                                                                                  AS NetAmount
             , td.NetAmount_TransCur                                                                                         AS NetAmount_TransCur
             , ISNULL(td.BaseAmount, 0) + ISNULL(td.CustomerCharge, 0) + ISNULL(td.DiscountAmount, 0)                        AS TotalAmount
             , ISNULL(td.BaseAmount_TransCur, 0) + ISNULL(td.CustomerCharge_TransCur, 0)
               + ISNULL(td.DiscountAmount_TransCur, 0)                                                                       AS TotalAmount_TransCur
             , td.OrderedQuantity_SalesUOM                                                                                   AS OrderedQuantity_SalesUOM
             , td.OrderedQuantity_SalesUOM * ISNULL(vuc.factor, 0)                                                           AS OrderedQuantity_FT

             , td.OrderedQuantity_SalesUOM * ISNULL(vuc2.factor, 0)                                                          AS OrderedQuantity_LB
             , ROUND(td.OrderedQuantity_SalesUOM * ISNULL(vuc3.factor, 0), 0)                                                AS OrderedQuantity_PC
             , td.OrderedQuantity_SalesUOM * ISNULL(vuc4.factor, 0)                                                          AS OrderedQuantity_SQIN

             , td.OrderedQuantity                                                                                            AS OrderedQuantity
             , td.ShippedAmount                                                                                              AS ShippedAmount
             , td.ShippedAmount_TransCur                                                                                     AS ShippedAmount_TransCur
             , td.ShippedQuantity_SalesUOM                                                                                   AS ShippedQuantity_SalesUOM
             , td.ShippedQuantity_SalesUOM * ISNULL(vuc.factor, 0)                                                           AS ShippedQuantity_FT

             , td.ShippedQuantity_SalesUOM * ISNULL(vuc2.factor, 0)                                                          AS ShippedQuantity_LB
             , ROUND(td.ShippedQuantity_SalesUOM * ISNULL(vuc3.factor, 0), 0)                                                AS ShippedQuantity_PC
             , td.ShippedQuantity_SalesUOM * ISNULL(vuc4.factor, 0)                                                          AS ShippedQuantity_SQIN

             , td.ShippedQuantity                                                                                            AS ShippedQuantity
             , td.RemainingAmount                                                                                            AS RemainingAmount
             , td.RemainingAmount_TransCur                                                                                   AS RemainingAmount_TransCur
             , td.RemainingQuantity_SalesUOM                                                                                 AS RemainingQuantity_SalesUOM
             , td.RemainingQuantity_SalesUOM * ISNULL(vuc.factor, 0)                                                         AS RemainingQuantity_FT

             , td.RemainingQuantity_SalesUOM * ISNULL(vuc2.factor, 0)                                                        AS RemainingQuantity_LB
             , ROUND(td.RemainingQuantity_SalesUOM * ISNULL(vuc3.factor, 0), 0)                                              AS RemainingQuantity_PC
             , td.RemainingQuantity_SalesUOM * ISNULL(vuc4.factor, 0)                                                        AS RemainingQuantity_SQIN

             , td.RemainingQuantity                                                                                          AS RemainingQuantity
             , td.PhysicalReservedQuantity                                                                                   AS PhysicalReservedQuantity
             , td.PhysicalReservedQuantity_SalesUOM                                                                          AS PhysicalReservedQuantity_SalesUOM
             , td.PhysicalReservedQuantity_SalesUOM * ISNULL(vuc.factor, 0)                                                  AS PhysicalReservedQuantity_FT

             , td.PhysicalReservedQuantity_SalesUOM * ISNULL(vuc2.factor, 0)                                                 AS PhysicalReservedQuantity_LB
             , td.PhysicalReservedQuantity_SalesUOM * ISNULL(vuc3.factor, 0)                                                 AS PhysicalReservedQuantity_M
             , td.PhysicalReservedQuantity_SalesUOM * ISNULL(vuc4.factor, 0)                                                 AS PhysicalReservedQuantity_MT
             , ROUND(td.PhysicalReservedQuantity_SalesUOM * ISNULL(vuc3.factor, 0), 0)                                       AS PhysicalReservedQuantity_PC

             , td.CustomerPO                                                                                                 AS CustomerPO
             , td.CustomerReference                                                                                          AS CustomerReference
             , td.ReturnItemID                                                                                               AS ReturnItemID
             , td._SourceDate                                                                                                AS _SourceDate
             , td._SourceID                                                                                                  AS _SourceID
             , td._RecID                                                                                                     AS _RecID

          FROM salesorderline_factdetailmain              td
          LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc
            ON vuc.legalentitykey  = td.LegalEntityKey
           AND vuc.productkey      = td.ProductKey
           AND vuc.fromuomkey      = td.SalesUOMKey
        -- AND vuc.touom           = 'FT'





          LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc2
            ON vuc2.legalentitykey = td.LegalEntityKey
           AND vuc2.productkey     = td.ProductKey
           AND vuc2.fromuomkey     = td.SalesUOMKey
        -- AND vuc2.touom          = 'LB'
          LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc3
            ON vuc3.legalentitykey = td.LegalEntityKey
           AND vuc3.productkey     = td.ProductKey
           AND vuc3.fromuomkey     = td.SalesUOMKey
        -- AND vuc3.touom          = 'PC'
          LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc4
            ON vuc4.legalentitykey = td.LegalEntityKey
           AND vuc4.productkey     = td.ProductKey
           AND vuc4.fromuomkey     = td.SalesUOMKey
        -- AND vuc4.touom          = 'SQIN'
),
salesorderline_factdetailbase AS (
    SELECT t.CurrencyKey                                                            AS CurrencyKey
             , t.CustomerKey                                                            AS CustomerKey
             , t.DeliveryAddressKey                                                     AS DeliveryAddressKey
             , t.DeliveryModeKey                                                        AS DeliveryModeKey
             , t.DeliveryTermKey                                                        AS DeliveryTermKey
             , t.DocumentStatusKey                                                      AS DocumentStatusKey
             , t.FinancialKey                                                           AS FinancialKey
             , t.InvoiceCustomerKey                                                     AS InvoiceCustomerKey
             , t.LegalEntityKey                                                         AS LegalEntityKey
             , t.LotKey                                                                 AS LotKey
             , t.OrderDateKey                                                           AS OrderDateKey
             , t.OnTimeShipStatusKey                                                    AS OnTimeShipStatusKey
             , t.PaymentTermKey                                                         AS PaymentTermKey
             , t.PickingUOMKey                                                          AS PickingUOMKey
             , t.PricingUOMKey                                                          AS PricingUOMKey
             , t.ParentProductKey                                                       AS BOMParentProductKey
             , t.ProductKey                                                             AS ProductKey
             , t.ProductionKey                                                          AS ProductionKey
             , t.ReceiptDateConfirmedKey                                                AS ReceiptDateConfirmedKey
             , t.ReceiptDateRequestedKey                                                AS ReceiptDateRequestedKey
             , t.ReturnReasonkey                                                        AS ReturnReasonkey
             , t.ReturnStatusKey                                                        AS ReturnStatusKey
             , t.ReservedDateKey                                                        AS ReservedDateKey
             , t.SalesAgreementLineKey                                                  AS SalesAgreementLineKey
             , t.SalesCategoryKey                                                       AS SalesCategoryKey
             , t.SalesLineStatusKey                                                     AS SalesLineStatusKey
             , t.SalesOrderLineKey                                                      AS SalesOrderLineKey
             , t.SalesOrderKey                                                          AS SalesOrderKey
             , t.SalesPersonKey                                                         AS SalesPersonKey
             , t.SalesStatusKey                                                         AS SalesStatusKey
             , t.SalesTakerKey                                                          AS SalesTakerKey
             , t.SalesTypeKey                                                           AS SalesTypeKey
             , t.SalesUOMKey                                                            AS SalesUOMKey
             , t.ShipDateActualKey                                                      AS ShipDateActualKey
             , t.ShipDateConfirmedKey                                                   AS ShipDateConfirmedKey
             , t.ShipDateDueKey                                                         AS ShipDateDueKey
             , t.ShipDateRequestedKey                                                   AS ShipDateRequestedKey
             , t.InventorySiteKey                                                       AS InventorySiteKey
             , t.TaxGroupKey                                                            AS TaxGroupKey
             , t.WarehouseKey                                                           AS WarehouseKey
             , t.UserInfoKey                                                            AS UserInfoKey
             , t.AdditionalCharge                                                       AS AdditionalCharge
             , t.AdditionalCharge_TransCur                                              AS AdditionalCharge_TransCur
             , t.BaseAmount                                                             AS BaseAmount
             , t.BaseAmount_TransCur                                                    AS BaseAmount_TransCur
             , t.BaseUnitPrice                                                          AS BaseUnitPrice
             , t.BaseUnitPrice_TransCur                                                 AS BaseUnitPrice_TransCur
             , t.CustomerCharge                                                         AS CustomerCharge
             , t.CustomerCharge_TransCur                                                AS CustomerCharge_TransCur
             , t.DaysLateTillDue                                                        AS DaysLateTillDue
             , t.DiscountAmount                                                         AS DiscountAmount
             , t.DiscountAmount_TransCur                                                AS DiscountAmount_TransCur
             , t.IncludedCharge                                                         AS IncludedCharge
             , t.IncludedCharge_TransCur                                                AS IncludedCharge_TransCur
             , t.NetAmount                                                              AS NetAmount
             , t.NetAmount_TransCur                                                     AS NetAmount_TransCur
             , t.NonBillableCharge                                                      AS NonBillableCharge
             , t.NonBillableCharge_TransCur                                             AS NonBillableCharge_TransCur
             , t.OpenLineCount                                                          AS OpenLineCount
             , CASE WHEN t.SalesStatusID = 1 THEN t.OrderedQuantity ELSE 0 END          AS OpenQuantity
             , CASE WHEN t.SalesStatusID = 1 THEN t.OrderedQuantity_SalesUOM ELSE 0 END AS OpenQuantity_SalesUOM
             , CASE WHEN t.SalesStatusID = 1 THEN t.OrderedQuantity_FT ELSE 0 END       AS OpenQuantity_FT

             , CASE WHEN t.SalesStatusID = 1 THEN t.OrderedQuantity_LB ELSE 0 END       AS OpenQuantity_LB
             , CASE WHEN t.SalesStatusID = 1 THEN t.OrderedQuantity_PC ELSE 0 END       AS OpenQuantity_PC
             , CASE WHEN t.SalesStatusID = 1 THEN t.OrderedQuantity_SQIN ELSE 0 END     AS OpenQuantity_SQIN

             , t.OrderedQuantity_SalesUOM                                               AS OrderedQuantity_SalesUOM
             , t.OrderedQuantity_FT                                                     AS OrderedQuantity_FT

             , t.OrderedQuantity_LB                                                     AS OrderedQuantity_LB
             , t.OrderedQuantity_PC                                                     AS OrderedQuantity_PC
             , t.OrderedQuantity_SQIN                                                   AS OrderedQuantity_SQIN

             , t.OrderedQuantity                                                        AS OrderedQuantity
             , t.ShippedAmount                                                          AS ShippedAmount
             , t.ShippedAmount_TransCur                                                 AS ShippedAmount_TransCur
             , t.ShippedQuantity_SalesUOM                                               AS ShippedQuantity_SalesUOM
             , t.OrderLineCount                                                         AS OrderLineCount
             , t.PriceUnit                                                              AS PriceUnit
             , t.RemainingQuantity_FT                                                   AS RemainingQuantity_FT

             , t.RemainingQuantity_LB                                                   AS RemainingQuantity_LB
             , t.RemainingQuantity_PC                                                   AS RemainingQuantity_PC
             , t.RemainingQuantity_SQIN                                                 AS RemainingQuantity_SQIN

             , t.RemainingQuantity                                                      AS RemainingQuantity
             , t.PhysicalReservedQuantity                                               AS PhysicalReservedQuantity
             , t.PhysicalReservedQuantity_SalesUOM                                      AS PhysicalReservedQuantity_SalesUOM
             , t.PhysicalReservedQuantity_FT                                            AS PhysicalReservedQuantity_FT

             , t.PhysicalReservedQuantity_LB                                            AS PhysicalReservedQuantity_LB
             , t.PhysicalReservedQuantity_M                                             AS PhysicalReservedQuantity_M
             , t.PhysicalReservedQuantity_MT                                            AS PhysicalReservedQuantity_MT
             , t.PhysicalReservedQuantity_PC                                            AS PhysicalReservedQuantity_PC

             , t.ReturnLineCount                                                        AS ReturnLineCount
             , t.TotalUnitPrice                                                         AS TotalUnitPrice
             , t.TotalUnitPrice_TransCur                                                AS TotalUnitPrice_TransCur
             , t.RemainingAmount                                                        AS RemainingAmount
             , t.RemainingAmount_TransCur                                               AS RemainingAmount_TransCur
             , t.RemainingQuantity_SalesUOM                                             AS RemainingQuantity_SalesUOM
             , t.ShippedQuantity_FT                                                     AS ShippedQuantity_FT

             , t.ShippedQuantity_LB                                                     AS ShippedQuantity_LB
             , t.ShippedQuantity_PC                                                     AS ShippedQuantity_PC
             , t.ShippedQuantity_SQIN                                                   AS ShippedQuantity_SQIN

             , t.ShippedQuantity                                                        AS ShippedQuantity
             , t.TotalAmount                                                            AS OrderedSalesAmount
             , t.TotalAmount_TransCur                                                   AS OrderedSalesAmount_TransCur
             , t.CustomerPO                                                             AS CustomerPO
             , t.CustomerReference                                                      AS CustomerReference
             , t.ReturnItemID                                                           AS ReturnItemID
             , le.AccountingCurrencyID
             , le.TransExchangeRateType
             , t._SourceDate
             , t._RecID                                                                 AS _RecID
             , t._SourceID                                                              AS _SourceID
             , CURRENT_TIMESTAMP AS _CreatedDate
             , CURRENT_TIMESTAMP AS _ModifiedDate

          FROM salesorderline_factdetail1                   t
          LEFT JOIN silver.cma_LegalEntity       le
            ON le.LegalEntityKey    = t.LegalEntityKey
),
salesorderline_factdetailcad AS (
    SELECT t.CurrencyKey                                                            AS CurrencyKey
             , t.CustomerKey                                                            AS CustomerKey
             , t.DeliveryAddressKey                                                     AS DeliveryAddressKey
             , t.DeliveryModeKey                                                        AS DeliveryModeKey
             , t.DeliveryTermKey                                                        AS DeliveryTermKey
             , t.DocumentStatusKey                                                      AS DocumentStatusKey
             , t.FinancialKey                                                           AS FinancialKey
             , t.InvoiceCustomerKey                                                     AS InvoiceCustomerKey
             , t.LegalEntityKey                                                         AS LegalEntityKey
             , t.LotKey                                                                 AS LotKey
             , t.OrderDateKey                                                           AS OrderDateKey
             , t.OnTimeShipStatusKey                                                    AS OnTimeShipStatusKey
             , t.PaymentTermKey                                                         AS PaymentTermKey
             , t.PickingUOMKey                                                          AS PickingUOMKey
             , t.PricingUOMKey                                                          AS PricingUOMKey
             , t.BOMParentProductKey                                                    AS BOMParentProductKey
             , t.ProductKey                                                             AS ProductKey
             , t.ProductionKey                                                          AS ProductionKey
             , t.ReceiptDateConfirmedKey                                                AS ReceiptDateConfirmedKey
             , t.ReceiptDateRequestedKey                                                AS ReceiptDateRequestedKey
             , t.ReturnReasonkey                                                        AS ReturnReasonkey
             , t.ReturnStatusKey                                                        AS ReturnStatusKey
             , t.ReservedDateKey                                                        AS ReservedDateKey
             , t.SalesAgreementLineKey                                                  AS SalesAgreementLineKey
             , t.SalesCategoryKey                                                       AS SalesCategoryKey
             , t.SalesLineStatusKey                                                     AS SalesLineStatusKey
             , t.SalesOrderLineKey                                                      AS SalesOrderLineKey
             , t.SalesOrderKey                                                          AS SalesOrderKey
             , t.SalesPersonKey                                                         AS SalesPersonKey
             , t.SalesStatusKey                                                         AS SalesStatusKey
             , t.SalesTakerKey                                                          AS SalesTakerKey
             , t.SalesTypeKey                                                           AS SalesTypeKey
             , t.SalesUOMKey                                                            AS SalesUOMKey
             , t.ShipDateActualKey                                                      AS ShipDateActualKey
             , t.ShipDateConfirmedKey                                                   AS ShipDateConfirmedKey
             , t.ShipDateDueKey                                                         AS ShipDateDueKey
             , t.ShipDateRequestedKey                                                   AS ShipDateRequestedKey
             , t.InventorySiteKey                                                       AS InventorySiteKey
             , t.TaxGroupKey                                                            AS TaxGroupKey
             , t.WarehouseKey                                                           AS WarehouseKey
             , t.UserInfoKey                                                            AS UserInfoKey
             , t.AdditionalCharge                                                       AS AdditionalCharge
             , t.AdditionalCharge * ISNULL(ex.ExchangeRate, 1)                          AS AdditionalCharge_CAD
             , t.AdditionalCharge_TransCur                                              AS AdditionalCharge_TransCur
             , t.BaseAmount                                                             AS BaseAmount
             , t.BaseAmount * ISNULL(ex.ExchangeRate, 1)                                AS BaseAmount_CAD
             , t.BaseAmount_TransCur                                                    AS BaseAmount_TransCur
             , t.BaseUnitPrice                                                          AS BaseUnitPrice
             , t.BaseUnitPrice * ISNULL(ex.ExchangeRate, 1)                             AS BaseUnitPrice_CAD
             , t.BaseUnitPrice_TransCur                                                 AS BaseUnitPrice_TransCur
             , t.CustomerCharge                                                         AS CustomerCharge
             , t.CustomerCharge * ISNULL(ex.ExchangeRate, 1)                            AS CustomerCharge_CAD
             , t.CustomerCharge_TransCur                                                AS CustomerCharge_TransCur
             , t.DaysLateTillDue                                                        AS DaysLateTillDue
             , t.DiscountAmount                                                         AS DiscountAmount
             , t.DiscountAmount * ISNULL(ex.ExchangeRate, 1)                            AS DiscountAmount_CAD
             , t.DiscountAmount_TransCur                                                AS DiscountAmount_TransCur
             , t.IncludedCharge                                                         AS IncludedCharge
             , t.IncludedCharge * ISNULL(ex.ExchangeRate, 1)                            AS IncludedCharge_CAD
             , t.IncludedCharge_TransCur                                                AS IncludedCharge_TransCur
             , t.NetAmount                                                              AS NetAmount
             , t.NetAmount * ISNULL(ex.ExchangeRate, 1)                                 AS NetAmount_CAD
             , t.NetAmount_TransCur                                                     AS NetAmount_TransCur
             , t.NonBillableCharge                                                      AS NonBillableCharge
             , t.NonBillableCharge * ISNULL(ex.ExchangeRate, 1)                         AS NonBillableCharge_CAD
             , t.NonBillableCharge_TransCur                                             AS NonBillableCharge_TransCur
             , t.OpenLineCount                                                          AS OpenLineCount
             , t.OpenQuantity                                                           AS OpenQuantity
             , t.OpenQuantity_SalesUOM                                                  AS OpenQuantity_SalesUOM
             , t.OpenQuantity_FT                                                        AS OpenQuantity_FT
             , t.OpenQuantity_LB                                                        AS OpenQuantity_LB
             , t.OpenQuantity_PC                                                        AS OpenQuantity_PC
             , t.OpenQuantity_SQIN                                                      AS OpenQuantity_SQIN
             , t.OrderedQuantity_SalesUOM                                               AS OrderedQuantity_SalesUOM
             , t.OrderedQuantity_FT                                                     AS OrderedQuantity_FT
             , t.OrderedQuantity_LB                                                     AS OrderedQuantity_LB
             , t.OrderedQuantity_PC                                                     AS OrderedQuantity_PC
             , t.OrderedQuantity_SQIN                                                   AS OrderedQuantity_SQIN
             , t.OrderedQuantity                                                        AS OrderedQuantity
             , t.ShippedAmount                                                          AS ShippedAmount
             , t.ShippedAmount * ISNULL(ex.ExchangeRate, 1)                             AS ShippedAmount_CAD
             , t.ShippedAmount_TransCur                                                 AS ShippedAmount_TransCur
             , t.ShippedQuantity_SalesUOM                                               AS ShippedQuantity_SalesUOM
             , t.OrderLineCount                                                         AS OrderLineCount
             , t.PriceUnit                                                              AS PriceUnit
             , t.RemainingQuantity_FT                                                   AS RemainingQuantity_FT
             , t.RemainingQuantity_LB                                                   AS RemainingQuantity_LB
             , t.RemainingQuantity_PC                                                   AS RemainingQuantity_PC
             , t.RemainingQuantity_SQIN                                                 AS RemainingQuantity_SQIN
             , t.RemainingQuantity                                                      AS RemainingQuantity
             , t.PhysicalReservedQuantity                                               AS PhysicalReservedQuantity
             , t.PhysicalReservedQuantity_SalesUOM                                      AS PhysicalReservedQuantity_SalesUOM
             , t.PhysicalReservedQuantity_FT                                            AS PhysicalReservedQuantity_FT
             , t.PhysicalReservedQuantity_LB                                            AS PhysicalReservedQuantity_LB
             , t.PhysicalReservedQuantity_M                                             AS PhysicalReservedQuantity_M
             , t.PhysicalReservedQuantity_MT                                            AS PhysicalReservedQuantity_MT
             , t.PhysicalReservedQuantity_PC                                            AS PhysicalReservedQuantity_PC
             , t.ReturnLineCount                                                        AS ReturnLineCount
             , t.TotalUnitPrice                                                         AS TotalUnitPrice
             , t.TotalUnitPrice * ISNULL(ex.ExchangeRate, 1)                            AS TotalUnitPrice_CAD
             , t.TotalUnitPrice_TransCur                                                AS TotalUnitPrice_TransCur
             , t.RemainingAmount                                                        AS RemainingAmount
             , t.RemainingAmount * ISNULL(ex.ExchangeRate, 1)                           AS RemainingAmount_CAD
             , t.RemainingAmount_TransCur                                               AS RemainingAmount_TransCur
             , t.RemainingQuantity_SalesUOM                                             AS RemainingQuantity_SalesUOM
             , t.ShippedQuantity_FT                                                     AS ShippedQuantity_FT
             , t.ShippedQuantity_LB                                                     AS ShippedQuantity_LB
             , t.ShippedQuantity_PC                                                     AS ShippedQuantity_PC
             , t.ShippedQuantity_SQIN                                                   AS ShippedQuantity_SQIN
             , t.ShippedQuantity                                                        AS ShippedQuantity
             , t.OrderedSalesAmount                                                     AS OrderedSalesAmount
             , t.OrderedSalesAmount * ISNULL(ex.ExchangeRate, 1)                        AS OrderedSalesAmount_CAD
             , t.OrderedSalesAmount_TransCur                                            AS OrderedSalesAmount_TransCur
             , t.CustomerPO                                                             AS CustomerPO
             , t.CustomerReference                                                      AS CustomerReference
             , t.ReturnItemID                                                           AS ReturnItemID
             , t.AccountingCurrencyID
             , t.TransExchangeRateType
             , t._SourceDate
             , t._RecID                                                                 AS _RecID
             , t._SourceID                                                              AS _SourceID
             , CURRENT_TIMESTAMP AS _CreatedDate
             , CURRENT_TIMESTAMP AS _ModifiedDate

          FROM salesorderline_factdetailbase                   t
          LEFT JOIN silver.cma_ExchangeRate_Fact ex
            ON ex.ExchangeDateKey   = t.OrderDateKey
           AND ex.FromCurrencyID    = t.AccountingCurrencyID
           AND ex.ToCurrencyID      = 'CAD'
           AND ex.ExchangeRateType  = t.TransExchangeRateType
),
salesorderline_factdetailmxp AS (
    SELECT t.CurrencyKey                                              AS CurrencyKey
             , t.CustomerKey                                                            AS CustomerKey
             , t.DeliveryAddressKey                                                     AS DeliveryAddressKey
             , t.DeliveryModeKey                                                        AS DeliveryModeKey
             , t.DeliveryTermKey                                                        AS DeliveryTermKey
             , t.DocumentStatusKey                                                      AS DocumentStatusKey
             , t.FinancialKey                                                           AS FinancialKey
             , t.InvoiceCustomerKey                                                     AS InvoiceCustomerKey
             , t.LegalEntityKey                                                         AS LegalEntityKey
             , t.LotKey                                                                 AS LotKey
             , t.OrderDateKey                                                           AS OrderDateKey
             , t.OnTimeShipStatusKey                                                    AS OnTimeShipStatusKey
             , t.PaymentTermKey                                                         AS PaymentTermKey
             , t.PickingUOMKey                                                          AS PickingUOMKey
             , t.PricingUOMKey                                                          AS PricingUOMKey
             , t.BOMParentProductKey                                                    AS BOMParentProductKey
             , t.ProductKey                                                             AS ProductKey
             , t.ProductionKey                                                          AS ProductionKey
             , t.ReceiptDateConfirmedKey                                                AS ReceiptDateConfirmedKey
             , t.ReceiptDateRequestedKey                                                AS ReceiptDateRequestedKey
             , t.ReturnReasonkey                                                        AS ReturnReasonkey
             , t.ReturnStatusKey                                                        AS ReturnStatusKey
             , t.ReservedDateKey                                                        AS ReservedDateKey
             , t.SalesAgreementLineKey                                                  AS SalesAgreementLineKey
             , t.SalesCategoryKey                                                       AS SalesCategoryKey
             , t.SalesLineStatusKey                                                     AS SalesLineStatusKey
             , t.SalesOrderLineKey                                                      AS SalesOrderLineKey
             , t.SalesOrderKey                                                          AS SalesOrderKey
             , t.SalesPersonKey                                                         AS SalesPersonKey
             , t.SalesStatusKey                                                         AS SalesStatusKey
             , t.SalesTakerKey                                                          AS SalesTakerKey
             , t.SalesTypeKey                                                           AS SalesTypeKey
             , t.SalesUOMKey                                                            AS SalesUOMKey
             , t.ShipDateActualKey                                                      AS ShipDateActualKey
             , t.ShipDateConfirmedKey                                                   AS ShipDateConfirmedKey
             , t.ShipDateDueKey                                                         AS ShipDateDueKey
             , t.ShipDateRequestedKey                                                   AS ShipDateRequestedKey
             , t.InventorySiteKey                                                       AS InventorySiteKey
             , t.TaxGroupKey                                                            AS TaxGroupKey
             , t.WarehouseKey                                                           AS WarehouseKey
             , t.UserInfoKey                                                            AS UserInfoKey
             , t.AdditionalCharge                                                       AS AdditionalCharge
             , t.AdditionalCharge_CAD                                                   AS AdditionalCharge_CAD
             , t.AdditionalCharge * ISNULL(ex.ExchangeRate, 1)                          AS AdditionalCharge_MXP
             , t.AdditionalCharge_TransCur                                              AS AdditionalCharge_TransCur
             , t.BaseAmount                                                             AS BaseAmount
             , t.BaseAmount_CAD                                                         AS BaseAmount_CAD
             , t.BaseAmount * ISNULL(ex.ExchangeRate, 1)                                AS BaseAmount_MXP
             , t.BaseAmount_TransCur                                                    AS BaseAmount_TransCur
             , t.BaseUnitPrice                                                          AS BaseUnitPrice
             , t.BaseUnitPrice_CAD                                                      AS BaseUnitPrice_CAD
             , t.BaseUnitPrice * ISNULL(ex.ExchangeRate, 1)                             AS BaseUnitPrice_MXP
             , t.BaseUnitPrice_TransCur                                                 AS BaseUnitPrice_TransCur
             , t.CustomerCharge                                                         AS CustomerCharge
             , t.CustomerCharge_CAD                                                     AS CustomerCharge_CAD
             , t.CustomerCharge * ISNULL(ex.ExchangeRate, 1)                            AS CustomerCharge_MXP
             , t.CustomerCharge_TransCur                                                AS CustomerCharge_TransCur
             , t.DaysLateTillDue                                                        AS DaysLateTillDue
             , t.DiscountAmount                                                         AS DiscountAmount
             , t.DiscountAmount_CAD                                                     AS DiscountAmount_CAD
             , t.DiscountAmount * ISNULL(ex.ExchangeRate, 1)                            AS DiscountAmount_MXP
             , t.DiscountAmount_TransCur                                                AS DiscountAmount_TransCur
             , t.IncludedCharge                                                         AS IncludedCharge
             , t.IncludedCharge_CAD                                                     AS IncludedCharge_CAD
             , t.IncludedCharge * ISNULL(ex.ExchangeRate, 1)                            AS IncludedCharge_MXP
             , t.IncludedCharge_TransCur                                                AS IncludedCharge_TransCur
             , t.NetAmount                                                              AS NetAmount
             , t.NetAmount_CAD                                                          AS NetAmount_CAD
             , t.NetAmount * ISNULL(ex.ExchangeRate, 1)                                 AS NetAmount_MXP
             , t.NetAmount_TransCur                                                     AS NetAmount_TransCur
             , t.NonBillableCharge                                                      AS NonBillableCharge
             , t.NonBillableCharge_CAD                                                  AS NonBillableCharge_CAD
             , t.NonBillableCharge * ISNULL(ex.ExchangeRate, 1)                         AS NonBillableCharge_MXP
             , t.NonBillableCharge_TransCur                                             AS NonBillableCharge_TransCur
             , t.OpenLineCount                                                          AS OpenLineCount
             , t.OpenQuantity                                                           AS OpenQuantity
             , t.OpenQuantity_SalesUOM                                                  AS OpenQuantity_SalesUOM
             , t.OpenQuantity_FT                                                        AS OpenQuantity_FT
             , t.OpenQuantity_LB                                                        AS OpenQuantity_LB
             , t.OpenQuantity_PC                                                        AS OpenQuantity_PC
             , t.OpenQuantity_SQIN                                                      AS OpenQuantity_SQIN
             , t.OrderedQuantity_SalesUOM                                               AS OrderedQuantity_SalesUOM
             , t.OrderedQuantity_FT                                                     AS OrderedQuantity_FT
             , t.OrderedQuantity_LB                                                     AS OrderedQuantity_LB
             , t.OrderedQuantity_PC                                                     AS OrderedQuantity_PC
             , t.OrderedQuantity_SQIN                                                   AS OrderedQuantity_SQIN
             , t.OrderedQuantity                                                        AS OrderedQuantity
             , t.ShippedAmount                                                          AS ShippedAmount
             , t.ShippedAmount_CAD                                                      AS ShippedAmount_CAD
             , t.ShippedAmount * ISNULL(ex.ExchangeRate, 1)                             AS ShippedAmount_MXP
             , t.ShippedAmount_TransCur                                                 AS ShippedAmount_TransCur
             , t.ShippedQuantity_SalesUOM                                               AS ShippedQuantity_SalesUOM
             , t.OrderLineCount                                                         AS OrderLineCount
             , t.PriceUnit                                                              AS PriceUnit
             , t.RemainingQuantity_FT                                                   AS RemainingQuantity_FT
             , t.RemainingQuantity_LB                                                   AS RemainingQuantity_LB
             , t.RemainingQuantity_PC                                                   AS RemainingQuantity_PC
             , t.RemainingQuantity_SQIN                                                 AS RemainingQuantity_SQIN
             , t.RemainingQuantity                                                      AS RemainingQuantity
             , t.PhysicalReservedQuantity                                               AS PhysicalReservedQuantity
             , t.PhysicalReservedQuantity_SalesUOM                                      AS PhysicalReservedQuantity_SalesUOM
             , t.PhysicalReservedQuantity_FT                                            AS PhysicalReservedQuantity_FT
             , t.PhysicalReservedQuantity_LB                                            AS PhysicalReservedQuantity_LB
             , t.PhysicalReservedQuantity_M                                             AS PhysicalReservedQuantity_M
             , t.PhysicalReservedQuantity_MT                                            AS PhysicalReservedQuantity_MT
             , t.PhysicalReservedQuantity_PC                                            AS PhysicalReservedQuantity_PC
             , t.ReturnLineCount                                                        AS ReturnLineCount
             , t.TotalUnitPrice                                                         AS TotalUnitPrice
             , t.TotalUnitPrice_CAD                                                     AS TotalUnitPrice_CAD
             , t.TotalUnitPrice * ISNULL(ex.ExchangeRate, 1)                            AS TotalUnitPrice_MXP
             , t.TotalUnitPrice_TransCur                                                AS TotalUnitPrice_TransCur
             , t.RemainingAmount                                                        AS RemainingAmount
             , t.RemainingAmount_CAD                                                    AS RemainingAmount_CAD
             , t.RemainingAmount * ISNULL(ex.ExchangeRate, 1)                           AS RemainingAmount_MXP
             , t.RemainingAmount_TransCur                                               AS RemainingAmount_TransCur
             , t.RemainingQuantity_SalesUOM                                             AS RemainingQuantity_SalesUOM
             , t.ShippedQuantity_FT                                                     AS ShippedQuantity_FT
             , t.ShippedQuantity_LB                                                     AS ShippedQuantity_LB
             , t.ShippedQuantity_PC                                                     AS ShippedQuantity_PC
             , t.ShippedQuantity_SQIN                                                   AS ShippedQuantity_SQIN
             , t.ShippedQuantity                                                        AS ShippedQuantity
             , t.OrderedSalesAmount                                                     AS OrderedSalesAmount
             , t.RemainingAmount_CAD                                                    AS OrderedSalesAmount_CAD
             , t.OrderedSalesAmount * ISNULL(ex.ExchangeRate, 1)                        AS OrderedSalesAmount_MXP
             , t.OrderedSalesAmount_TransCur                                            AS OrderedSalesAmount_TransCur
             , t.CustomerPO                                                             AS CustomerPO
             , t.CustomerReference                                                      AS CustomerReference
             , t.ReturnItemID                                                           AS ReturnItemID
             , t.AccountingCurrencyID
             , t.TransExchangeRateType
             , t._SourceDate
             , t._RecID                                                                 AS _RecID
             , t._SourceID                                                              AS _SourceID
             , CURRENT_TIMESTAMP AS _CreatedDate
             , CURRENT_TIMESTAMP AS _ModifiedDate

          FROM salesorderline_factdetailcad                   t
          LEFT JOIN silver.cma_ExchangeRate_Fact ex
            ON ex.ExchangeDateKey   = t.OrderDateKey
           AND ex.FromCurrencyID    = t.AccountingCurrencyID
           AND ex.ToCurrencyID      = 'MXP'
           AND ex.ExchangeRateType  = t.TransExchangeRateType
)
SELECT t.CurrencyKey                                                            AS CurrencyKey
         , t.CustomerKey                                                            AS CustomerKey
         , t.DeliveryAddressKey                                                     AS DeliveryAddressKey
         , t.DeliveryModeKey                                                        AS DeliveryModeKey
         , t.DeliveryTermKey                                                        AS DeliveryTermKey
         , t.DocumentStatusKey                                                      AS DocumentStatusKey
         , t.FinancialKey                                                           AS FinancialKey
         , t.InvoiceCustomerKey                                                     AS InvoiceCustomerKey
         , t.LegalEntityKey                                                         AS LegalEntityKey
         , t.LotKey                                                                 AS LotKey
         , t.OrderDateKey                                                           AS OrderDateKey
         , t.OnTimeShipStatusKey                                                    AS OnTimeShipStatusKey
         , t.PaymentTermKey                                                         AS PaymentTermKey
         , t.PickingUOMKey                                                          AS PickingUOMKey
         , t.PricingUOMKey                                                          AS PricingUOMKey
         , t.BOMParentProductKey                                                    AS BOMParentProductKey
         , t.ProductKey                                                             AS ProductKey
         , t.ProductionKey                                                          AS ProductionKey
         , t.ReceiptDateConfirmedKey                                                AS ReceiptDateConfirmedKey
         , t.ReceiptDateRequestedKey                                                AS ReceiptDateRequestedKey
         , t.ReturnReasonkey                                                        AS ReturnReasonkey
         , t.ReturnStatusKey                                                        AS ReturnStatusKey
         , t.ReservedDateKey                                                        AS ReservedDateKey
         , t.SalesAgreementLineKey                                                  AS SalesAgreementLineKey
         , t.SalesCategoryKey                                                       AS SalesCategoryKey
         , t.SalesLineStatusKey                                                     AS SalesLineStatusKey
         , t.SalesOrderLineKey                                                      AS SalesOrderLineKey
         , t.SalesOrderKey                                                          AS SalesOrderKey
         , t.SalesPersonKey                                                         AS SalesPersonKey
         , t.SalesStatusKey                                                         AS SalesStatusKey
         , t.SalesTakerKey                                                          AS SalesTakerKey
         , t.SalesTypeKey                                                           AS SalesTypeKey
         , t.SalesUOMKey                                                            AS SalesUOMKey
         , t.ShipDateActualKey                                                      AS ShipDateActualKey
         , t.ShipDateConfirmedKey                                                   AS ShipDateConfirmedKey
         , t.ShipDateDueKey                                                         AS ShipDateDueKey
         , t.ShipDateRequestedKey                                                   AS ShipDateRequestedKey
         , t.InventorySiteKey                                                       AS InventorySiteKey
         , t.TaxGroupKey                                                            AS TaxGroupKey
         , t.WarehouseKey                                                           AS WarehouseKey
         , t.UserInfoKey                                                            AS UserInfoKey
         , t.AdditionalCharge                                                       AS AdditionalCharge
         , t.AdditionalCharge_CAD                                                   AS AdditionalCharge_CAD
         , t.AdditionalCharge_MXP                                                   AS AdditionalCharge_MXP
         , t.AdditionalCharge * ISNULL(ex.ExchangeRate, 1)                          AS AdditionalCharge_USD
         , t.AdditionalCharge_TransCur                                              AS AdditionalCharge_TransCur
         , t.BaseAmount                                                             AS BaseAmount
         , t.BaseAmount_CAD                                                         AS BaseAmount_CAD
         , t.BaseAmount_MXP                                                         AS BaseAmount_MXP
         , t.BaseAmount * ISNULL(ex.ExchangeRate, 1)                                AS BaseAmount_USD
         , t.BaseAmount_TransCur                                                    AS BaseAmount_TransCur
         , t.BaseUnitPrice                                                          AS BaseUnitPrice
         , t.BaseUnitPrice_CAD                                                      AS BaseUnitPrice_CAD
         , t.BaseUnitPrice_MXP                                                      AS BaseUnitPrice_MXP
         , t.BaseUnitPrice * ISNULL(ex.ExchangeRate, 1)                             AS BaseUnitPrice_USD
         , t.BaseUnitPrice_TransCur                                                 AS BaseUnitPrice_TransCur
         , t.CustomerCharge                                                         AS CustomerCharge
         , t.CustomerCharge_CAD                                                     AS CustomerCharge_CAD
         , t.CustomerCharge_MXP                                                     AS CustomerCharge_MXP
         , t.CustomerCharge * ISNULL(ex.ExchangeRate, 1)                            AS CustomerCharge_USD
         , t.CustomerCharge_TransCur                                                AS CustomerCharge_TransCur
         , t.DaysLateTillDue                                                        AS DaysLateTillDue
         , t.DiscountAmount                                                         AS DiscountAmount
         , t.DiscountAmount_CAD                                                     AS DiscountAmount_CAD
         , t.DiscountAmount_MXP                                                     AS DiscountAmount_MXP
         , t.DiscountAmount * ISNULL(ex.ExchangeRate, 1)                            AS DiscountAmount_USD
         , t.DiscountAmount_TransCur                                                AS DiscountAmount_TransCur
         , t.IncludedCharge                                                         AS IncludedCharge
         , t.IncludedCharge_CAD                                                     AS IncludedCharge_CAD
         , t.IncludedCharge_MXP                                                     AS IncludedCharge_MXP
         , t.IncludedCharge * ISNULL(ex.ExchangeRate, 1)                            AS IncludedCharge_USD
         , t.IncludedCharge_TransCur                                                AS IncludedCharge_TransCur
         , t.NetAmount                                                              AS NetAmount
         , t.NetAmount_CAD                                                          AS NetAmount_CAD
         , t.NetAmount_MXP                                                          AS NetAmount_MXP
         , t.NetAmount * ISNULL(ex.ExchangeRate, 1)                                 AS NetAmount_USD
         , t.NetAmount_TransCur                                                     AS NetAmount_TransCur
         , t.NonBillableCharge                                                      AS NonBillableCharge
         , t.NonBillableCharge_CAD                                                  AS NonBillableCharge_CAD
         , t.NonBillableCharge_MXP                                                  AS NonBillableCharge_MXP
         , t.NonBillableCharge * ISNULL(ex.ExchangeRate, 1)                         AS NonBillableCharge_USD
         , t.NonBillableCharge_TransCur                                             AS NonBillableCharge_TransCur
         , t.OpenLineCount                                                          AS OpenLineCount
         , t.OpenQuantity                                                           AS OpenQuantity
         , t.OpenQuantity_SalesUOM                                                  AS OpenQuantity_SalesUOM
         , t.OpenQuantity_FT                                                        AS OpenQuantity_FT
         , t.OpenQuantity_LB                                                        AS OpenQuantity_LB
         , t.OpenQuantity_PC                                                        AS OpenQuantity_PC
         , t.OpenQuantity_SQIN                                                      AS OpenQuantity_SQIN
         , t.OrderedQuantity_SalesUOM                                               AS OrderedQuantity_SalesUOM
         , t.OrderedQuantity_FT                                                     AS OrderedQuantity_FT
         , t.OrderedQuantity_LB                                                     AS OrderedQuantity_LB
         , t.OrderedQuantity_PC                                                     AS OrderedQuantity_PC
         , t.OrderedQuantity_SQIN                                                   AS OrderedQuantity_SQIN
         , t.OrderedQuantity                                                        AS OrderedQuantity
         , t.ShippedAmount                                                          AS ShippedAmount
         , t.ShippedAmount_CAD                                                      AS ShippedAmount_CAD
         , t.ShippedAmount_MXP                                                      AS ShippedAmount_MXP
         , t.ShippedAmount * ISNULL(ex.ExchangeRate, 1)                             AS ShippedAmount_USD
         , t.ShippedAmount_TransCur                                                 AS ShippedAmount_TransCur
         , t.ShippedQuantity_SalesUOM                                               AS ShippedQuantity_SalesUOM
         , t.OrderLineCount                                                         AS OrderLineCount
         , t.PriceUnit                                                              AS PriceUnit
         , t.RemainingQuantity_FT                                                   AS RemainingQuantity_FT
         , t.RemainingQuantity_LB                                                   AS RemainingQuantity_LB
         , t.RemainingQuantity_PC                                                   AS RemainingQuantity_PC
         , t.RemainingQuantity_SQIN                                                 AS RemainingQuantity_SQIN
         , t.RemainingQuantity                                                      AS RemainingQuantity
         , t.PhysicalReservedQuantity                                               AS PhysicalReservedQuantity
         , t.PhysicalReservedQuantity_SalesUOM                                      AS PhysicalReservedQuantity_SalesUOM
         , t.PhysicalReservedQuantity_FT                                            AS PhysicalReservedQuantity_FT
         , t.PhysicalReservedQuantity_LB                                            AS PhysicalReservedQuantity_LB
         , t.PhysicalReservedQuantity_M                                             AS PhysicalReservedQuantity_M
         , t.PhysicalReservedQuantity_MT                                            AS PhysicalReservedQuantity_MT
         , t.PhysicalReservedQuantity_PC                                            AS PhysicalReservedQuantity_PC
         , t.ReturnLineCount                                                        AS ReturnLineCount
         , t.TotalUnitPrice                                                         AS TotalUnitPrice
         , t.TotalUnitPrice_CAD                                                     AS TotalUnitPrice_CAD
         , t.TotalUnitPrice_MXP                                                     AS TotalUnitPrice_MXP
         , t.TotalUnitPrice * ISNULL(ex.ExchangeRate, 1)                            AS TotalUnitPrice_USD
         , t.TotalUnitPrice_TransCur                                                AS TotalUnitPrice_TransCur
         , t.RemainingAmount                                                        AS RemainingAmount
         , t.RemainingAmount_CAD                                                    AS RemainingAmount_CAD
         , t.RemainingAmount_MXP                                                    AS RemainingAmount_MXP
         , t.RemainingAmount * ISNULL(ex.ExchangeRate, 1)                           AS RemainingAmount_USD
         , t.RemainingAmount_TransCur                                               AS RemainingAmount_TransCur
         , t.RemainingQuantity_SalesUOM                                             AS RemainingQuantity_SalesUOM
         , t.ShippedQuantity_FT                                                     AS ShippedQuantity_FT
         , t.ShippedQuantity_LB                                                     AS ShippedQuantity_LB
         , t.ShippedQuantity_PC                                                     AS ShippedQuantity_PC
         , t.ShippedQuantity_SQIN                                                   AS ShippedQuantity_SQIN
         , t.ShippedQuantity                                                        AS ShippedQuantity
         , t.OrderedSalesAmount                                                     AS OrderedSalesAmount
         , t.OrderedSalesAmount_CAD                                                 AS OrderedSalesAmount_CAD
         , t.OrderedSalesAmount_MXP                                                 AS OrderedSalesAmount_MXP
         , t.OrderedSalesAmount * ISNULL(ex.ExchangeRate, 1)                        AS OrderedSalesAmount_USD
         , t.OrderedSalesAmount_TransCur                                            AS OrderedSalesAmount_TransCur
         , t.CustomerPO                                                             AS CustomerPO
         , t.CustomerReference                                                      AS CustomerReference
         , t.ReturnItemID                                                           AS ReturnItemID
         , t.AccountingCurrencyID
         , t.TransExchangeRateType
         , t._SourceDate
         , t._RecID                                                                 AS _RecID
         , t._SourceID                                                              AS _SourceID
         , CURRENT_TIMESTAMP AS _CreatedDate
         , CURRENT_TIMESTAMP AS _ModifiedDate

      FROM salesorderline_factdetailmxp                   t
      LEFT JOIN silver.cma_ExchangeRate_Fact ex
        ON ex.ExchangeDateKey   = t.OrderDateKey
       AND ex.FromCurrencyID    = t.AccountingCurrencyID
       AND ex.ToCurrencyID      = 'USD'
       AND ex.ExchangeRateType  = t.TransExchangeRateType
