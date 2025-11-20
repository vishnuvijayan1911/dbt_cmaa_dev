{{ config(materialized='table', tags=['silver'], alias='purchaseorderline_fact') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseorderline_f/purchaseorderline_f.py
-- Root method: PurchaseorderlineFact.purchaseorderline_factdetail [PurchaseOrderLine_FactDetail]
-- Inlined methods: PurchaseorderlineFact.purchaseorderline_factapprover [PurchaseOrderLine_FactApprover], PurchaseorderlineFact.purchaseorderline_factreceipt [PurchaseOrderLine_FactReceipt], PurchaseorderlineFact.purchaseorderline_factinventtransorigin [PurchaseOrderLine_FactInventTransOrigin], PurchaseorderlineFact.purchaseorderline_factmarkednotdistributed [PurchaseOrderLine_FactMarkedNotDistributed], PurchaseorderlineFact.purchaseorderline_factstage [PurchaseOrderLine_FactStage], PurchaseorderlineFact.purchaseorderline_factcharge [PurchaseOrderLine_FactCharge], PurchaseorderlineFact.purchaseorderline_factexhangerate [PurchaseOrderLine_FactExhangeRate], PurchaseorderlineFact.purchaseorderline_factdetailmain [PurchaseOrderLine_FactDetailMain]
-- external_table_name: PurchaseOrderLine_FactDetail
-- schema_name: temp

WITH
purchaseorderline_factapprover AS (
    SELECT t.*

          FROM (   SELECT wft.[USER]
                        , ROW_NUMBER() OVER (PARTITION BY pt.recid
    ORDER BY pt.recid  )    AS RankVal
                        , pt.recid
                        , wft.trackingcontext
                        , wft.trackingtype
                     FROM {{ ref('purchtable') }}                       pt
                    INNER JOIN {{ ref('workflowtrackingstatustable') }} wfs
                       ON wfs.contextrecid                = pt.recid
                    INNER JOIN {{ ref('sqldictionary') }}               sd
                       ON sd.fieldid                      = 0
                      AND sd.tabid                        = wfs.contexttableid
                      AND sd.name                         = 'PurchTable'
                     LEFT JOIN {{ ref('workflowtrackingtable') }}       wft
                       ON wft.workflowtrackingstatustable = wfs.recid
                    WHERE wft.trackingcontext = 2 
                      AND wft.trackingtype    = 8 
          ) t
         WHERE RankVal <= 2;
),
purchaseorderline_factreceipt AS (
    SELECT pl.recid              AS RecID
             , MAX(vpst.deliverydate) AS ReceivedDate
             , SUM(vpst.qty)          AS ReceivedQuantity_PurchUOM
             , SUM(vpst.inventqty)    AS ReceivedQuantity
             , SUM(vpst.lineamount_w) AS ReceivedAmount

          FROM {{ ref('purchline') }}                 pl
              INNER JOIN {{ ref('vendpackingsliptrans') }} vpst
            ON vpst.dataareaid   = pl.dataareaid
           AND vpst.inventtransid = pl.inventtransid
           AND vpst.inventdimid   = pl.inventdimid
         GROUP BY pl.recid;
),
purchaseorderline_factinventtransorigin AS (
    SELECT ito.itemid  AS ItemID
    			 ,pl.inventtransid
             , ito.recid   AS RecID_ITO
             , pl.recid    AS RecID_PL
             , SUM(it.qty) AS QTY_IT

          FROM {{ ref('purchtable') }}             pt
         INNER JOIN {{ ref('purchline') }}         pl
            ON pl.dataareaid        = pt.dataareaid
           AND pl.purchid           = pt.purchid
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid       = pl.dataareaid
           AND ito.inventtransid    = pl.inventtransid
           AND ito.itemid           = pl.itemid
         INNER JOIN {{ ref('inventtrans') }}       it
            ON it.inventtransorigin = ito.recid
           AND it.dataareaid        = ito.dataareaid
           AND (it.statusreceipt IN ( 1, 2, 3, 4, 5 ) OR it.statusissue IN ( 1, 2, 3, 4, 5, 6 ))
         WHERE pl.purchstatus = 1
         GROUP BY ito.recid
                , ito.itemid
                , pl.recid
    			 ,pl.inventtransid
                , pl.linenumber;
),
purchaseorderline_factmarkednotdistributed AS (
    SELECT  DISTINCT tq.InventTransID
             , ito.dataareaid
             , tq.RecID_PL AS RecID_PL

          FROM {{ ref('inventtrans') }}            it
          JOIN purchaseorderline_factinventtransorigin         tq
            ON (tq.RecID_ITO = it.markingrefinventtransorigin OR it.markingrefinventtransorigin = 0)
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON ito.itemid    = tq.ItemID
           AND ito.recid     = it.inventtransorigin
         WHERE it.valueopen                   = 1
           AND it.transchildtype              = 0
           AND it.packingslipreturned         = 0
           AND it.datephysical                = '1900-01-01 00:00:00.000'
           AND it.markingrefinventtransorigin <> 0
           AND it.statusreceipt               <> 3
           AND ito.referencecategory          = 0;
),
purchaseorderline_factstage AS (
    SELECT pl.recid                                                                                          AS RecID
             , pl.deliverypostaladdress                                                                           AS DeliveryPostalAddress
             , pl.dataareaid                                                                                     AS LegalEntityID
             , CAST(pl.createddatetime AS DATE)                                                                  AS OrderDate
             , CAST(pl.deliverydate AS DATE)                                                                      AS DeliveryDate
             , CAST(pl.confirmeddlv AS DATE)                                                                      AS DeliveryDateConfirmed
             , tr.ReceivedDate                                                                                    AS DeliveryDateActual
             , ta1.[USER]                                                                                          AS Approver1
             , ta2.[USER]                                                                                         AS Approver2
             , it.product                                                                                         AS ProductID
             , pl.itemid                                                                                          AS ItemID
             , id.inventcolorid                                                                                   AS ProductLength
             , id.inventsizeid                                                                                    AS ProductWidth
             , id.inventstyleid                                                                                   AS ProductColor
             , id.configid                                                                                        AS ProductConfig
             , id.inventsiteid                                                                                    AS SiteID
             , id.inventlocationid                                                                                AS WarehouseID
             , pt.orderaccount                                                                                    AS VendorAccount
             , pt.invoiceaccount                                                                                  AS InvoiceAccount
             , pl.defaultdimension                                                                                AS DefaultDimension
             , pl.currencycode                                                                                    AS CurrencyID
             , pt.workerpurchplacer                                                                               AS BuyerID
             , pt.documentstate                                                                                   AS ApprovalStatusID
             , pt.documentstatus                                                                                  AS DocumentStatusID
             , pt.dlvterm                                                                                         AS DeliveryTermID
             , pt.dlvmode                                                                                         AS DeliveryModeID
             , pt.payment                                                                                         AS PaymentTermID
             , pt.taxgroup                                                                                        AS TaxGroupID
             , pl.purchstatus                                                                                     AS PurchaseLineStatusID
             , pt.purchstatus                                                                                     AS PurchaseStatusID
             , pl.purchreqid                                                                                      AS PurchaseRequisitionID
             , pl.purchasetype                                                                                    AS PurchaseTypeID
             , ito.recid                                                                                         AS RecID_ITO
             , pt.returnreasoncodeid                                                                              AS ReturnReasonID
             , pl.returnstatus                                                                                    AS ReturnStatusID
             , pt.returnitemnum                                                                                   AS ReturnItemID
             , pl.priceunit                                                                                       AS PriceUnit
             , pl.purchprice                                                                                      AS BaseUnitPrice_TransCur
             , pl.cmatotalprice                                                                                   AS TotalUnitPrice_TransCur
             , pl.purchunit                                                                                AS PurchaseUnit
             , pl.cmapriceuom                                                                              AS PricingUnit
             , pl.cmareceivingunit                                                                         AS ReceivingUnit
             , im.unitid                                                                                   AS InventoryUnit
             , pl.purchprice * pl.purchqty / ISNULL(NULLIF(pl.priceunit, 0), 1)                                   AS BaseAmount_TransCur
             , ISNULL(NULLIF(pl.cmatotalamount, 0), pl.lineamount)                                                AS OrderedPurchaseAmount_TransCur
             , pl.lineamount                                                                                      AS NetAmount_TransCur
             , pl.purchqty                                                                                        AS OrderedQuantity_PurchUOM
             , pl.qtyordered                                                                                      AS OrderedQuantity
             , tr.ReceivedAmount                                                                                  AS ReceivedAmount_TransCur
             , tr.ReceivedQuantity_PurchUOM                                                                       AS ReceivedQuantity_PurchUOM
             , tr.ReceivedQuantity                                                                                AS ReceivedQuantity
             , CASE WHEN pl.purchstatus IN ( 1, 2 )
                    THEN pl.remainpurchphysical * ISNULL(NULLIF(pl.cmatotalprice, 0), pl.purchprice)
                         / ISNULL(NULLIF(pl.priceunit, 0), 1)
                    ELSE NULL END                                                                                 AS RemainingAmount_TransCur
             , CASE WHEN pl.purchstatus IN ( 1, 2 ) THEN pl.remainpurchphysical ELSE NULL END                     AS RemainingQuantity_PurchUOM
             , CASE WHEN pl.purchstatus IN ( 1, 2 ) THEN pl.remaininventphysical ELSE NULL END                    AS RemainingQuantity
             , CASE WHEN pl.purchstatus IN ( 1, 2 ) THEN pl.remaininventfinancial ELSE NULL END                   AS ReceivedNotInvoicedQuantity
             , CASE WHEN pl.purchstatus IN ( 1, 2 ) THEN pl.remainpurchfinancial ELSE NULL END                    AS ReceivedNotInvoicedQuantity_PurchUOM
             , CAST(CASE WHEN pl.purchasetype = 4 THEN 1 ELSE 0 END AS SMALLINT)                                  AS ReturnLineCount
             , CAST(CASE WHEN pl.purchasetype = 3 THEN 1 ELSE 0 END AS SMALLINT)                                  AS PurchaseLineCount
             , CAST(CASE WHEN pl.purchasetype = 3 AND tr.ReceivedDate > '1/1/1900' THEN 1 ELSE 0 END AS SMALLINT) AS ReceivedLineCount
             , CAST(1 AS SMALLINT)                                                                                AS OrderLineCount
             , CASE WHEN pl.purchasetype = 3
                     AND YEAR(tr.ReceivedDate) > 1900
                    THEN DATEDIFF(DAY, tr.ReceivedDate, SYSDATETIME()) END                                        AS ReceivedNotInvoicedDays
             , CASE WHEN pl.purchstatus NOT BETWEEN 1 AND 3
                    THEN NULL
                    WHEN pl.purchasetype NOT IN ( 3, 4 ) 
                    THEN NULL
                    WHEN pl.purchasetype = 4 
                    THEN 7
                    WHEN (tr.ReceivedDate IS NULL OR CAST(tr.ReceivedDate AS DATE) <= '1/1/1900')
                     AND pl.purchstatus IN ( 2, 3 ) 
                    THEN NULL
                    WHEN (   COALESCE(NULLIF(pl.confirmeddlv, '1/1/1900'), pl.deliverydate) IS NULL
                        OR   CAST(COALESCE(NULLIF(pl.confirmeddlv, '1/1/1900'), pl.deliverydate) AS DATE) <= '1/1/1900')
                     AND pl.purchstatus = 1 
                    THEN 6 
                    WHEN (   COALESCE(NULLIF(pl.confirmeddlv, '1/1/1900'), pl.deliverydate) IS NULL
                        OR   CAST(COALESCE(NULLIF(pl.confirmeddlv, '1/1/1900'), pl.deliverydate) AS DATE) <= '1/1/1900')
                     AND pl.purchstatus <> 1 
                    THEN 5 
                    WHEN pl.purchstatus = 1 
                     AND CAST(COALESCE(NULLIF(pl.confirmeddlv, '1/1/1900'), pl.deliverydate) AS DATE) >= CAST(SYSDATETIME() AS DATE)
                    THEN 1 
                    WHEN pl.purchstatus = 1 
                     AND CAST(COALESCE(NULLIF(pl.confirmeddlv, '1/1/1900'), pl.deliverydate) AS DATE) < CAST(SYSDATETIME() AS DATE)
                    THEN 2 
                    WHEN pl.purchstatus IN ( 2, 3 ) 
                     AND CAST(COALESCE(NULLIF(pl.confirmeddlv, '1/1/1900'), pl.deliverydate) AS DATE) >= CAST(tr.ReceivedDate AS DATE)
                    THEN 4 
                    WHEN pl.purchstatus IN ( 2, 3 ) 
                     AND CAST(COALESCE(NULLIF(pl.confirmeddlv, '1/1/1900'), pl.deliverydate) AS DATE) < CAST(tr.ReceivedDate AS DATE)
                    THEN 3 
                    ELSE NULL END                                                                                 AS OnTimeDeliveryStatusID
             , prl.recid                                                                                         AS RecID_PRL
             , pl.procurementcategory                                                                             AS RecID_PC
             , al.recid                                                                                           AS RecID_AL
             , pl.modifieddatetime                                                                               AS _SourceDate
             , mnt.InventTransID                                                                                  AS InventTransID_Sales

          FROM {{ ref('purchline') }}              pl
         INNER JOIN {{ ref('purchtable') }}        pt
            ON pt.dataareaid          = pl.dataareaid
           AND pt.purchid              = pl.purchid
           AND pt.purchstatus          <> 4 
          LEFT JOIN {{ ref('inventtable') }}       it
            ON it.dataareaid          = pl.dataareaid
           AND it.itemid               = pl.itemid
          LEFT JOIN {{ ref('inventtablemodule') }} im
            ON im.dataareaid          = it.dataareaid
           AND im.itemid               = it.itemid
           AND im.moduletype           = 0
          LEFT JOIN {{ ref('purchreqline') }}      prl
            ON prl.inventdimiddataarea = pl.dataareaid
           AND prl.linerefid           = pl.purchreqlinerefid
          LEFT JOIN {{ ref('inventdim') }}         id
            ON id.dataareaid          = pl.dataareaid
           AND id.inventdimid          = pl.inventdimid
          LEFT JOIN purchaseorderline_factreceipt              tr
            ON tr.RECID                = pl.recid
          LEFT JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid         = pl.dataareaid
           AND ito.inventtransid       = pl.inventtransid
          LEFT JOIN purchaseorderline_factapprover             ta1
            ON ta1.RECID              = pt.recid
           AND ta1.RankVal             = 1
          LEFT JOIN purchaseorderline_factapprover             ta2
            ON ta2.RECID              = pt.recid
           AND ta2.RankVal             = 2
          LEFT JOIN purchaseorderline_factmarkednotdistributed mnt
            ON mnt.RecID_PL            = pl.recid
          LEFT JOIN {{ ref('agreementline') }}     al
            ON al.recid         = pl.matchingagreementline
         WHERE pl.purchstatus <> 4 
           AND pl.purchasetype IN ( 3, 4 );
),
purchaseorderline_factcharge AS (
    SELECT pol.PurchaseOrderLineKey            AS PurchaseOrderLineKey
             , SUM(crg.IncludedCharge)             AS IncludedCharge
             , SUM(crg.IncludedCharge_TransCur)    AS IncludedCharge_TransCur
             , SUM(crg.AdditionalCharge)           AS AdditionalCharge
             , SUM(crg.AdditionalCharge_TransCur)  AS AdditionalCharge_TransCur
             , SUM(crg.NonBillableCharge)          AS NonBillableCharge
             , SUM(crg.NonBillableCharge_TransCur) AS NonBillableCharge_TransCur

          FROM {{ ref('purchaseorderlinecharge_f') }} crg
         INNER JOIN {{ ref('purchaseorderline_d') }}       pol
            ON pol.PurchaseOrderLineKey = crg.PurchaseOrderLineKey
           AND pol._SourceID            = 1
         GROUP BY pol.PurchaseOrderLineKey;
),
purchaseorderline_factexhangerate AS (
    SELECT t.*

          FROM (   SELECT ts.RecID     AS RecID_PL
                        , vij.exchrate/100 AS ExchangeRate
                        , ROW_NUMBER() OVER (PARTITION BY pl.recid
    ORDER BY vij.invoicedate DESC)     AS Rank_val
                     FROM purchaseorderline_factstage                    ts
                    INNER JOIN {{ ref('purchline') }}        pl
                       ON pl.recid               = ts.RecID
                     LEFT JOIN {{ ref('vendinvoicetrans') }} vit
                       ON vit.dataareaid         = pl.dataareaid
                      AND vit.inventtransid       = pl.inventtransid
                      AND vit.itemid              = pl.itemid
                    INNER JOIN {{ ref('vendinvoicejour') }}  vij
                       ON vij.dataareaid         = vit.dataareaid
                      AND vij.purchid             = vit.purchid
                      AND vij.invoiceid           = vit.invoiceid
                      AND vij.invoicedate         = vit.invoicedate
                      AND vij.numbersequencegroup = vit.numbersequencegroup
                      AND vij.internalinvoiceid   = vit.internalinvoiceid
                    WHERE ts.PurchaseLineStatusID = 3) AS t
         WHERE t.Rank_val = 1;
),
purchaseorderline_factdetailmain AS (
    SELECT dpol.PurchaseOrderLineKey
             , dpo.PurchaseOrderKey                                                                     AS PurchaseOrderKey
             , prl.purchaserequisitionlinekey                                                           AS PurchaseRequisitionLineKey
             , pal.PurchaseAgreementLineKey                                                             AS PurchaseAgreementLineKey
             , le.LegalEntityKey                                                                        AS LegalEntityKey
             , dc.CurrencyKey                                                                           AS CurrencyKey
             , u.UserInfoKey                                                                            AS Approver1Key
             , u1.UserInfoKey                                                                           AS Approver2Key
             , de.EmployeeKey                                                                           AS BuyerKey
             , da.AddressKey                                                                            AS DeliveryAddressKey
             , dd1.DateKey                                                                              AS DeliveryDateKey
             , dd2.DateKey                                                                              AS DeliveryDateConfirmedKey
             , dd3.DateKey                                                                              AS DeliveryDateActualKey
             , ddm.DeliveryModeKey                                                                      AS DeliveryModeKey
             , ddt.DeliveryTermKey                                                                      AS DeliveryTermKey
             , dds.DocumentStatusKey                                                                    AS DocumentStatusKey
             , fd1.FinancialKey                                                                         AS FinancialKey
             , dv1.VendorKey                                                                            AS VendorKey
             , dd.DateKey                                                                               AS OrderDateKey
             , pa.PaymentTermKey                                                                        AS PaymentTermKey
             , ISNULL(dp.ProductKey, -1)                                                                AS ProductKey
             , dpas.PurchaseApprovalStatusKey                                                           AS PurchaseApprovalStatusKey
             , dps.PurchaseStatusKey                                                                    AS PurchaseStatusKey
             , dps1.PurchaseStatusKey                                                                   AS PurchaseLineStatusKey
             , dpt.PurchaseTypeKey                                                                      AS PurchaseTypeKey
             , du.UOMKey                                                                                AS PricingUOMKey
             , dpc.ProcurementCategoryKey                                                               AS ProcurementCategoryKey
             , du1.UOMKey                                                                               AS PurchaseUOMKey
             , du2.UOMKey                                                                               AS ReceivingUOMKey
             , drr.ReturnReasonKey                                                                      AS ReturnReasonKey
             , drs.ReturnStatusKey                                                                      AS ReturnStatusKey
             , dsol.SalesOrderLineKey                                                                   AS SalesOrderLineKey
             , tg.TaxGroupKey                                                                           AS TaxGroupKey
             , dw.WarehouseKey                                                                          AS WarehouseKey
             , it.lotkey                                                                                AS LotKey
             , ds.InventorySiteKey                                                                      AS InventorySiteKey
             , dv2.VendorKey                                                                            AS InvoiceVendorKey
             , ts.ReturnLineCount                                                                       AS ReturnLineCount
             , ts.PurchaseLineCount                                                                     AS PurchaseLineCount
             , ts.ReceivedLineCount                                                                     AS ReceivedLineCount
             , CASE WHEN ts.OnTimeDeliveryStatusID IN ( 1, 2 )
                    THEN DATEDIFF(
                             DAY
                           , COALESCE(NULLIF(ts.DeliveryDateConfirmed, '1/1/1900'), ts.DeliveryDate)
                           , CAST(SYSDATETIME() AS DATE))
                    WHEN ts.OnTimeDeliveryStatusID IN ( 3, 4 )
                    THEN DATEDIFF(
                             DAY
                           , COALESCE(NULLIF(ts.DeliveryDateConfirmed, '1/1/1900'), ts.DeliveryDate)
                           , ts.DeliveryDateActual)
                    ELSE NULL END                                                                       AS DaysLateTillDue
             , ts.ReceivedNotInvoicedDays                                                               AS ReceivedNotInvoicedDays
             , ot.OnTimeDeliveryStatusKey                                                               AS OnTimeDeliveryStatusKey
             , le.TransExchangeRateType                                                                 AS TransExchangeRateType
             , ts.PriceUnit                                                                             AS PriceUnit
             , ts.BaseAmount_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1)                   AS BaseAmount
             , ts.BaseAmount_TransCur                                                                   AS BaseAmount_TransCur
             , ts.BaseUnitPrice_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1)                AS BaseUnitPrice
             , ts.BaseUnitPrice_TransCur                                                                AS BaseUnitPrice_TransCur
             , ts.TotalUnitPrice_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1)               AS TotalUnitPrice
             , ts.TotalUnitPrice_TransCur                                                               AS TotalUnitPrice_TransCur
             , le.AccountingCurrencyID                                                                  AS AccountingCurrencyID
             , ts.OrderedPurchaseAmount_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1)        AS OrderedPurchaseAmount
             , ts.OrderedPurchaseAmount_TransCur                                                        AS OrderedPurchaseAmount_TransCur
             , ts.NetAmount_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1)                    AS NetAmount
             , ts.NetAmount_TransCur                                                                    AS NetAmount_TransCur
             , ts.OrderedQuantity_PurchUOM                                                              AS OrderedQuantity_PurchUOM
             , ts.OrderedQuantity                                                                       AS OrderedQuantity
             , (((CASE WHEN ts.BaseUnitPrice_TransCur = 0
                       THEN CASE WHEN ts.OrderedPurchaseAmount_TransCur = 0
                                 THEN 0
                                 ELSE
             ( (ts.NetAmount_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1))
               / ISNULL(NULLIF(ts.OrderedQuantity_PurchUOM, 0), 1)) / (ISNULL(NULLIF(ts.PriceUnit, 0), 1)) END
                       ELSE ts.BaseUnitPrice_TransCur END)
                 * (CASE WHEN ts.OrderedQuantity_PurchUOM = 0 THEN 1 ELSE ts.OrderedQuantity_PurchUOM END)
                 / (ISNULL(NULLIF(ts.PriceUnit, 0), 1))) - ts.NetAmount_TransCur
                * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1))                                        AS DiscountAmount
             , (((CASE WHEN ts.BaseUnitPrice_TransCur = 0
                       THEN CASE WHEN ts.OrderedPurchaseAmount_TransCur = 0
                                 THEN 0
                                 ELSE
             ( ts.NetAmount_TransCur / ISNULL(NULLIF(ts.OrderedQuantity_PurchUOM, 0), 1))
             / (ISNULL(NULLIF(ts.PriceUnit, 0), 1)) END
                       ELSE ts.BaseUnitPrice_TransCur END)
                 * (CASE WHEN ts.OrderedQuantity_PurchUOM = 0 THEN 1 ELSE ts.OrderedQuantity_PurchUOM END)
                 / (ISNULL(NULLIF(ts.PriceUnit, 0), 1)))) - ts.NetAmount_TransCur                       AS DiscountAmount_TransCur
             , tca.IncludedCharge                                                                       AS IncludedCharge
             , tca.IncludedCharge_TransCur                                                              AS IncludedCharge_TransCur
             , tca.AdditionalCharge                                                                     AS AdditionalCharge
             , tca.AdditionalCharge_TransCur                                                            AS AdditionalCharge_TransCur
             , tca.NonBillableCharge                                                                    AS NonBillableCharge
             , tca.NonBillableCharge_TransCur                                                           AS NonBillableCharge_TransCur
             , ts.ReceivedAmount_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1)               AS ReceivedAmount
             , ts.ReceivedAmount_TransCur                                                               AS ReceivedAmount_TransCur
             , ts.ReceivedQuantity_PurchUOM                                                             AS ReceivedQuantity_PurchUOM
             , ts.ReceivedQuantity                                                                      AS ReceivedQuantity
             , (ISNULL(ts.ReceivedNotInvoicedQuantity, 0) * ts.BaseUnitPrice_TransCur
                / (ISNULL(NULLIF(ts.PriceUnit, 0), 1))) * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1) AS ReceivedNotInvoicedAmount
             , (ISNULL(ts.ReceivedNotInvoicedQuantity, 0) * ts.BaseUnitPrice_TransCur
                / (ISNULL(NULLIF(ts.PriceUnit, 0), 1)))                                                 AS ReceivedNotInvoicedAmount_TransCur
             , ts.ReceivedNotInvoicedQuantity_PurchUOM                                                  AS ReceivedNotInvoicedQuantity_PurchUOM
             , ts.ReceivedNotInvoicedQuantity                                                           AS ReceivedNotInvoicedQuantity
             , ts.RemainingAmount_TransCur * COALESCE(er.ExchangeRate, ex.ExchangeRate, 1)              AS RemainingAmount
             , ts.RemainingAmount_TransCur                                                              AS RemainingAmount_TransCur
             , ts.RemainingQuantity_PurchUOM                                                            AS RemainingQuantity_PurchUOM
             , ts.RemainingQuantity                                                                     AS RemainingQuantity
             , ISNULL((tca.IncludedCharge + tca.AdditionalCharge), 0)                                   AS VendorCharge
             , ISNULL((tca.IncludedCharge_TransCur + tca.AdditionalCharge_TransCur), 0)                 AS VendorCharge_TransCur
             , ts.PurchaseStatusID                                                                      AS PurchaseStatusID
             , ts.PurchaseTypeID                                                                        AS PurchaseTypeID
             , ts.InventoryUnit                                                                         AS InventoryUnit
             , ts.PurchaseLineStatusID                                                                  AS PurchaseLineStatusID
             , ts.ReturnItemID                                                                          AS ReturnItemID
             , ts.RecID                                                                                 AS _RecID
             , ts._SourceDate                                                                           AS _SourceDate
             , 1                                                                                        AS _SourceID

          FROM purchaseorderline_factstage                           ts
         INNER JOIN {{ ref('purchaseorderline_d') }}       dpol
            ON dpol._RecID                   = ts.RecID
           AND dpol._SourceID                = 1
         INNER JOIN {{ ref('legalentity_d') }}             le
            ON le.LegalEntityID              = ts.LegalEntityID
          LEFT JOIN {{ ref('address_d') }}                 da
            ON da._RecID                     = ts.DeliveryPostalAddress
           AND da._SourceID                  = 1
          LEFT JOIN {{ ref('product_d') }}                 dp
            ON dp.LegalEntityID              = ts.LegalEntityID
           AND dp.ItemID                     = ts.ItemID
           AND dp.ProductWidth               = ts.ProductWidth
           AND dp.ProductLength              = ts.ProductLength
           AND dp.ProductColor               = ts.ProductColor
           AND dp.ProductConfig              = ts.ProductConfig
          LEFT JOIN {{ ref('ontimedeliverystatus_d') }}    ot
            ON ot.OnTimeDeliveryStatusID     = ts.OnTimeDeliveryStatusID
          LEFT JOIN {{ ref('date_d') }}                    dd
            ON dd.Date                       = ts.OrderDate
          LEFT JOIN {{ ref('date_d') }}                    dd1
            ON dd1.Date                      = ts.DeliveryDate
          LEFT JOIN {{ ref('date_d') }}                    dd2
            ON dd2.Date                      = ts.DeliveryDateConfirmed
          LEFT JOIN {{ ref('date_d') }}                    dd3
            ON dd3.Date                      = ts.DeliveryDateActual
          LEFT JOIN {{ ref('vendor_d') }}                  dv1
            ON dv1.LegalEntityID             = ts.LegalEntityID
           AND dv1.VendorAccount             = ts.VendorAccount
          LEFT JOIN {{ ref('vendor_d') }}                  dv2
            ON dv2.LegalEntityID             = ts.LegalEntityID
           AND dv2.VendorAccount             = ts.InvoiceAccount
          LEFT JOIN {{ ref('inventorysite_d') }}           ds
            ON ds.LegalEntityID              = ts.LegalEntityID
           AND ds.InventorySiteID            = ts.SiteID
          LEFT JOIN {{ ref('warehouse_d') }}               dw
            ON dw.LegalEntityID              = ts.LegalEntityID
           AND dw.WarehouseID                = ts.WarehouseID
          LEFT JOIN {{ ref('purchaseorder_d') }}           dpo
            ON dpo.LegalEntityID             = dpol.LegalEntityID
           AND dpo.PurchaseOrderID           = dpol.PurchaseOrderID
          LEFT JOIN {{ ref('lot_d') }}                     it
            ON it._recid                     = ts.RecID_ITO
           AND it._sourceid                  = 1
          LEFT JOIN {{ ref('financial_d') }}               fd1
            ON fd1._RecID                    = ts.DefaultDimension
           AND fd1._SourceID                 = 1
          LEFT JOIN {{ ref('documentstatus_d') }}          dds
            ON dds.DocumentStatusID          = ts.DocumentStatusID
          LEFT JOIN {{ ref('salesorderline_d') }}          dsol
            ON dsol.LegalEntityID            = ts.LegalEntityID
           AND dsol.LotID                    = ts.InventTransID_Sales
          LEFT JOIN {{ ref('purchasestatus_d') }}          dps
            ON dps.PurchaseStatusID          = ts.PurchaseStatusID
          LEFT JOIN {{ ref('purchasestatus_d') }}          dps1
            ON dps1.PurchaseStatusID         = ts.PurchaseLineStatusID
          LEFT JOIN {{ ref('purchasetype_d') }}            dpt
            ON dpt.PurchaseTypeID            = ts.PurchaseTypeID
          LEFT JOIN {{ ref('purchaseapprovalstatus_d') }}  dpas
            ON dpas.PurchaseApprovalStatusID = ts.ApprovalStatusID
          LEFT JOIN {{ ref('deliverymode_d') }}            ddm
            ON ddm.LegalEntityID             = ts.LegalEntityID
           AND ddm.DeliveryModeID            = ts.DeliveryModeID
          LEFT JOIN {{ ref('purchaserequisitionline_d') }} prl
            ON prl._recid                    = ts.RecID_PRL
           AND prl._sourceid                 = 1
          LEFT JOIN {{ ref('deliveryterm_d') }}            ddt
            ON ddt.LegalEntityID             = ts.LegalEntityID
           AND ddt.DeliveryTermID            = ts.DeliveryTermID
          LEFT JOIN {{ ref('paymentterm_d') }}             pa
            ON pa.LegalEntityID              = ts.LegalEntityID
           AND pa.PaymentTermID              = ts.PaymentTermID
          LEFT JOIN {{ ref('taxgroup_d') }}                tg
            ON tg.LegalEntityID              = ts.LegalEntityID
           AND tg.TaxGroupID                 = ts.TaxGroupID
          LEFT JOIN {{ ref('uom_d') }}                     du
            ON du.UOM                        = ts.PricingUnit
          LEFT JOIN {{ ref('uom_d') }}                     du1
            ON du1.UOM                       = ts.PurchaseUnit
          LEFT JOIN {{ ref('uom_d') }}                     du2
            ON du2.UOM                       = ts.ReceivingUnit
          LEFT JOIN {{ ref('currency_d') }}                dc
            ON dc.CurrencyID                 = ts.CurrencyID
          LEFT JOIN purchaseorderline_factcharge                     tca
            ON tca.PurchaseOrderLineKey      = dpol.PurchaseOrderLineKey
          LEFT JOIN {{ ref('returnstatus_d') }}            drs
            ON drs.ReturnStatusID            = ts.ReturnStatusID
          LEFT JOIN {{ ref('returnreason_d') }}            drr
            ON drr.LegalEntityID             = ts.LegalEntityID
           AND drr.ReturnReasonID            = ts.ReturnReasonID
          LEFT JOIN {{ ref('procurementcategory_d') }}     dpc
            ON dpc._RecID                    = ts.RecID_PC
           AND dpc._SourceID                 = 1
          LEFT JOIN purchaseorderline_factexhangerate                er
            ON er.RecID_PL                   = ts.RECID
          LEFT JOIN {{ ref('date_d') }}                    dd4
            ON dd4.Date                      = CAST(GETDATE() AS DATE)
          LEFT JOIN {{ ref('exchangerate_f') }}       ex
            ON ex.ExchangeDateKey            = dd4.DateKey
           AND ex.FromCurrencyID             = ts.CurrencyID
           AND ex.ToCurrencyID               = le.AccountingCurrencyID
           AND ex.ExchangeRateType           = le.TransExchangeRateType
          LEFT JOIN {{ ref('userinfo_d') }}                u
            ON u.LegalEntityID               = ts.LegalEntityID
           AND u.UserName                    = ts.Approver1
          LEFT JOIN {{ ref('userinfo_d') }}                u1
            ON u1.LegalEntityID              = ts.LegalEntityID
           AND u1.UserName                   = ts.Approver2
          LEFT JOIN {{ ref('employee_d') }}                de
            ON de._RecID                     = ts.BuyerID
           AND de._SourceID                  = 1
          LEFT JOIN {{ ref('purchaseagreementline_d') }} pal
            ON pal._RecID            = ts.RecID_AL;
)
SELECT DISTINCT td.PurchaseOrderLineKey
         , td.PurchaseAgreementLineKey
         , td.PurchaseOrderKey
         , td.Approver1Key
         , td.Approver2Key
         , td.BuyerKey
         , td.CurrencyKey
         , td.DeliveryAddressKey
         , td.DeliveryDateActualKey
         , td.DeliveryDateConfirmedKey
         , td.DeliveryDateKey
         , td.DeliveryModeKey
         , td.DeliveryTermKey
         , td.DocumentStatusKey
         , td.FinancialKey
         , td.InvoiceVendorKey
         , td.LegalEntityKey
         , td.LotKey
         , td.OnTimeDeliveryStatusKey
         , td.OrderDateKey
         , td.PaymentTermKey
         , td.PricingUOMKey
         , td.ProcurementCategoryKey
         , td.ProductKey
         , td.PurchaseRequisitionLineKey
         , td.PurchaseApprovalStatusKey
         , td.PurchaseLineStatusKey
         , td.PurchaseStatusKey
         , td.PurchaseTypeKey
         , td.PurchaseUOMKey
         , td.ReceivingUOMKey
         , td.ReturnReasonKey
         , td.ReturnStatusKey
         , td.SalesOrderLineKey
         , td.InventorySiteKey
         , td.TaxGroupKey
         , td.VendorKey
         , td.WarehouseKey
         , td.AdditionalCharge
         , td.AdditionalCharge_TransCur
         , td.BaseAmount
         , td.BaseAmount_TransCur
         , td.BaseUnitPrice
         , td.BaseUnitPrice_TransCur
         , td.DaysLateTillDue
         , CASE WHEN td.DiscountAmount < 0 THEN td.DiscountAmount * -1 ELSE td.DiscountAmount END                         AS DiscountAmount
         , CASE WHEN td.DiscountAmount_TransCur < 0 THEN td.DiscountAmount_TransCur * -1 ELSE
                                                                                         td.DiscountAmount_TransCur END   AS DiscountAmount_TransCur
         , td.IncludedCharge                                                                                              AS IncludedCharge
         , td.IncludedCharge_TransCur                                                                                     AS IncludedCharge_TransCur
         , td.NetAmount                                                                                                   AS NetAmount
         , td.NetAmount_TransCur                                                                                          AS NetAmount_TransCur
         , td.NonBillableCharge                                                                                           AS NonBillableCharge
         , td.NonBillableCharge_TransCur                                                                                  AS NonBillableCharge_TransCur
         , CASE WHEN td.PurchaseTypeID <> 3 THEN NULL ELSE
                                                      CASE WHEN td.PurchaseLineStatusID IN ( 1, 2 ) THEN 1 ELSE 0 END END AS OpenLineCount
         , td.OrderedQuantity_PurchUOM * ISNULL(vuc6.factor, 1)                                                                      AS OrderedQuantity
         , td.OrderedQuantity_PurchUOM                                                                                    AS OrderedQuantity_PurchUOM
         , td.OrderedQuantity_PurchUOM * ISNULL(vuc.factor, 1)                                                                      AS OrderedQuantity_FT

         , td.OrderedQuantity_PurchUOM * ISNULL(vuc2.factor, 1)                                                                      AS OrderedQuantity_LB
         , ROUND(td.OrderedQuantity_PurchUOM * ISNULL(vuc3.factor, 1) , 0)                                                            AS OrderedQuantity_PC
         , td.OrderedQuantity_PurchUOM * ISNULL(vuc4.factor, 1)                                                                       AS OrderedQuantity_SQIN

         , td.PriceUnit                                                                                                   AS PriceUnit
         , td.PurchaseLineCount                                                                                           AS PurchaseLineCount
         , td.ReceivedAmount                                                                                              AS ReceivedAmount
         , td.ReceivedAmount_TransCur                                                                                     AS ReceivedAmount_TransCur
         , td.ReceivedNotInvoicedAmount                                                                                   AS ReceivedNotInvoicedAmount
         , td.ReceivedNotInvoicedAmount_TransCur                                                                          AS ReceivedNotInvoicedAmount_TransCur
         , td.ReceivedNotInvoicedDays                                                                                     AS ReceivedNotInvoicedDays
         , CASE WHEN td.ReceivedLineCount = 0 THEN NULL ELSE CASE WHEN td.PurchaseLineStatusID = 2 THEN 1 ELSE 0 END END  AS ReceivedNotInvoicedLineCount
         , td.ReceivedNotInvoicedQuantity                                                                                 AS ReceivedNotInvoicedQuantity
         , td.ReceivedNotInvoicedQuantity_PurchUOM                                                                        AS ReceivedNotInvoicedQuantity_PurchUOM
         , td.ReceivedNotInvoicedQuantity_PurchUOM * ISNULL(vuc.factor, 1)                                                            AS ReceivedNotInvoicedQuantity_FT
         , td.ReceivedNotInvoicedQuantity_PurchUOM * ISNULL(vuc.factor, 1) * 12                                                          AS ReceivedNotInvoicedQuantity_IN
         , td.ReceivedNotInvoicedQuantity_PurchUOM * ISNULL(vuc2.factor, 1)                                                          AS ReceivedNotInvoicedQuantity_LB
         , ROUND(td.ReceivedNotInvoicedQuantity_PurchUOM * ISNULL(vuc3.factor, 1) , 0)                                                AS ReceivedNotInvoicedQuantity_PC
         , td.ReceivedNotInvoicedQuantity_PurchUOM * ISNULL(vuc4.factor, 1)                                                           AS ReceivedNotInvoicedQuantity_SQIN
         , td.ReceivedNotInvoicedQuantity_PurchUOM * ISNULL(vuc2.factor, 1) * 0.0005                                                           AS ReceivedNotInvoicedQuantity_TON
         , td.ReceivedQuantity                                                                                            AS ReceivedQuantity
         , td.ReceivedQuantity_PurchUOM                                                                                   AS ReceivedQuantity_PurchUOM
         , td.ReceivedQuantity_PurchUOM * ISNULL(vuc.factor, 1)                                                                      AS ReceivedQuantity_FT

         , td.ReceivedQuantity_PurchUOM * ISNULL(vuc2.factor, 1)                                                                      AS ReceivedQuantity_LB
         , ROUND(td.ReceivedQuantity_PurchUOM * ISNULL(vuc3.factor, 1) , 0)                                                           AS ReceivedQuantity_PC
         , td.ReceivedQuantity_PurchUOM * ISNULL(vuc4.factor, 1)                                                                      AS ReceivedQuantity_SQIN

         , td.RemainingAmount                                                                                             AS RemainingAmount
         , td.RemainingAmount_TransCur                                                                                    AS RemainingAmount_TransCur
         , td.RemainingQuantity                                                                                           AS RemainingQuantity
         , td.RemainingQuantity_PurchUOM                                                                                  AS RemainingQuantity_PurchUOM
         , td.RemainingQuantity_PurchUOM * ISNULL(vuc.factor, 1)                                                                      AS RemainingQuantity_FT

         , td.RemainingQuantity_PurchUOM * ISNULL(vuc2.factor, 1)                                                                    AS RemainingQuantity_LB
         , ROUND(td.RemainingQuantity_PurchUOM * ISNULL(vuc3.factor, 1) , 0)                                                          AS RemainingQuantity_PC
         , td.RemainingQuantity_PurchUOM * ISNULL(vuc4.factor, 1)                                                                     AS RemainingQuantity_SQIN

         , td.ReturnLineCount                                                                                             AS ReturnLineCount
         , ISNULL(td.BaseAmount, 0) + ISNULL(td.VendorCharge, 0) + ISNULL(td.DiscountAmount, 0)                           AS OrderedPurchaseAmount
         , ISNULL(td.BaseAmount_TransCur, 0) + ISNULL(td.VendorCharge_TransCur, 0)
           + ISNULL(td.DiscountAmount_TransCur, 0)                                                                        AS OrderedPurchaseAmount_TransCur
         , td.TotalUnitPrice
         , td.TotalUnitPrice_TransCur
         , td.VendorCharge
         , td.VendorCharge_TransCur                                                                                       AS VendorCharge_TransCur
         , td.ReturnItemID                                                                                                AS ReturnItemID
         , td._SourceDate
         , td._RecID
         , td._SourceID

         ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))  AS  _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
      FROM purchaseorderline_factdetailmain              td
      LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc
        ON vuc.legalentitykey  = td.LegalEntityKey
       AND vuc.productkey      = td.ProductKey
       AND vuc.fromuomkey      = td.PurchaseUOMKey
    -- AND vuc.touom           = 'FT'





      LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc2
        ON vuc2.legalentitykey = td.LegalEntityKey
       AND vuc2.productkey     = td.ProductKey
       AND vuc2.fromuomkey     = td.PurchaseUOMKey
    -- AND vuc2.touom          = 'LB'
      LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc3
        ON vuc3.legalentitykey = td.LegalEntityKey
       AND vuc3.productkey     = td.ProductKey
       AND vuc3.fromuomkey     = td.PurchaseUOMKey
    -- AND vuc3.touom          = 'PC'
      LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc4
        ON vuc4.legalentitykey = td.LegalEntityKey
       AND vuc4.productkey     = td.ProductKey
       AND vuc4.fromuomkey     = td.PurchaseUOMKey
    -- AND vuc4.touom          = 'SQIN'





      LEFT JOIN {{ ref('vwuomconversion') }} vuc6
        ON vuc6.legalentitykey = td.LegalEntityKey
       AND vuc6.productkey     = td.ProductKey
       AND vuc6.fromuomkey     = td.PurchaseUOMKey
       AND vuc6.touom          = td.InventoryUnit;
