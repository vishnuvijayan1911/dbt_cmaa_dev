{{ config(materialized='table', tags=['silver'], alias='purchaseorder_fact') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseorder_f/purchaseorder_f.py
-- Root method: PurchaseorderFact.purchaseorder_factdetail [PurchaseOrder_FactDetail]
-- Inlined methods: PurchaseorderFact.purchaseorder_factreceipt [PurchaseOrder_FactReceipt], PurchaseorderFact.purchaseorder_factline [PurchaseOrder_FactLine], PurchaseorderFact.purchaseorder_factstage [PurchaseOrder_FactStage], PurchaseorderFact.purchaseorder_factlinecharge [PurchaseOrder_FactLineCharge], PurchaseorderFact.purchaseorder_factheadercharge [PurchaseOrder_FactHeaderCharge], PurchaseorderFact.purchaseorder_factdetailmain [PurchaseOrder_FactDetailMain]
-- external_table_name: PurchaseOrder_FactDetail
-- schema_name: temp

WITH
purchaseorder_factreceipt AS (
    SELECT pt.recid               AS RecID
             , MIN (vpst.deliverydate) AS ReceivedDate
             , SUM (vpst.lineamount_w) AS ReceivedAmount

          FROM {{ ref('purchtable') }}                pt

         INNER JOIN {{ ref('purchline') }}            pl
            ON pl.dataareaid     = pt.dataareaid
           AND pl.purchid         = pt.purchid
         INNER JOIN {{ ref('vendpackingsliptrans') }} vpst
            ON vpst.dataareaid   = pl.dataareaid
           AND vpst.inventtransid = pl.inventtransid
           AND vpst.inventdimid   = pl.inventdimid
         GROUP BY pt.recid;
),
purchaseorder_factline AS (
    SELECT pt.recid                                                                             AS RecID
             , SUM (pl.purchprice * pl.purchqty / ISNULL (NULLIF(pl.priceunit, 0), 1))               AS BaseAmount_TransCur
             , SUM (ISNULL (NULLIF(pl.cmatotalamount, 0), pl.lineamount))                            AS TotalAmount_TransCur
             , SUM (pl.lineamount)                                                                   AS NetAmount_TransCur
             , SUM (pl.remainpurchphysical * pl.cmatotalprice / ISNULL (NULLIF(pl.priceunit, 0), 1)) AS RemainingAmount_TransCur
             , SUM (ISNULL (pl.remaininventfinancial, 0) * pl.purchprice)                            AS ReceivedNotInvoicedAmount_TransCur
          FROM {{ ref('purchtable') }}     pt
         INNER JOIN {{ ref('purchline') }} pl
            ON pl.dataareaid = pt.dataareaid
           AND pl.purchid     = pt.purchid
           AND pl.purchstatus <> 4 
           AND pl.purchasetype IN ( 3, 4 ) 
         GROUP BY pt.recid;
),
purchaseorder_factstage AS (
    SELECT pt.itembuyergroupid                                                          AS BuyerGroupID
             , pt.workerpurchplacer                                                         AS BuyerID
             , pt.currencycode                                                              AS CurrencyID
             , pt.deliverypostaladdress                                                     AS DeliveryPostalAddress
             , CAST(pt.createddatetime AS DATE)                                            AS OrderDate
             , tr.ReceivedDate                                                              AS DeliveryDateActual
             , CAST(pt.confirmeddlv AS DATE)                                                AS DeliveryDateConfirmed
             , CAST(pt.deliverydate AS DATE)                                                AS DeliveryDate
             , pt.dlvmode                                                                   AS DeliveryModeID
             , pt.dlvterm                                                                   AS DeliveryTermID
             , pt.documentstate                                                             AS DocumentStateID
             , pt.documentstatus                                                            AS DocumentStatusID
             , pt.invoiceaccount                                                            AS InvoiceAccount
             , pt.dataareaid                                                               AS LegalEntityID
             , pt.paymmode                                                                  AS PaymentModeID
             , pt.payment                                                                   AS PaymentTermID
             , pt.purchstatus                                                               AS PurchaseStatusID
             , pt.purchasetype                                                              AS PurchaseTypeID
             , pt.requester                                                                 AS RequesterID
             , pt.returnreasoncodeid                                                        AS ReturnReasonID
             , pt.inventsiteid                                                              AS SiteID
             , pt.taxgroup                                                                  AS TaxGroupID
             , pt.orderaccount                                                              AS VendorAccount
             , pt.inventlocationid                                                          AS WarehouseID
             , tl.BaseAmount_TransCur                                                       AS BaseAmount_TransCur
             , pt.cashdisc                                                                  AS CashDiscountID
             , CAST(pt.cashdiscpercent AS NUMERIC(20, 6))                                   AS CashDiscountPercent
             , tl.TotalAmount_TransCur                                                      AS TotalAmount_TransCur
             , tl.NetAmount_TransCur                                                        AS NetAmount_TransCur
             , tl.RemainingAmount_TransCur                                                  AS RemainingAmount_TransCur
             , tr.ReceivedAmount                                                            AS ReceivedAmount_TransCur
             , CAST(pt.enddisc AS MONEY)                                                    AS DiscountAmount_TransCur
             , tl.ReceivedNotInvoicedAmount_TransCur                                        AS ReceivedNotInvoicedAmount_TransCur
             , CAST(CASE WHEN pt.purchasetype <> 3
                           OR pt.purchstatus = 4
                         THEN NULL
                         ELSE
                         CASE WHEN NULLIF(CAST(pt.deliverydate AS DATE), '1/1/1900') IS NULL
                              THEN CASE WHEN pt.purchstatus = 1 THEN 'OND' ELSE 'DND' END
                              ELSE
                              CASE WHEN (CASE WHEN pt.purchasetype = 3
                                               AND CAST(pt.deliverydate AS DATE) > '1/1/1900'
                                              THEN CASE WHEN pt.purchstatus = 1
                                                        THEN DATEDIFF (DAY, CAST(pt.deliverydate AS DATE), SYSDATETIME ())
                                                        ELSE
                                                        CASE WHEN pt.purchstatus IN ( 2, 3 )
                                                              AND tr.ReceivedDate > '1/1/1900'
                                                             THEN DATEDIFF (
                                                                      DAY, CAST(pt.deliverydate AS DATE), tr.ReceivedDate)
                                                             ELSE 0 END END END) <= 0
                                    AND pt.purchstatus = 1
                                   THEN 'NYD'
                                   ELSE
                                   CASE WHEN pt.purchstatus = 1
                                        THEN 'L'
                                        ELSE
                                        CASE WHEN (CASE WHEN pt.purchasetype = 3
                                                         AND CAST(pt.deliverydate AS DATE) > '1/1/1900'
                                                        THEN CASE WHEN pt.purchstatus = 1
                                                                  THEN DATEDIFF (
                                                                           DAY
                                                                           , CAST(pt.deliverydate AS DATE)
                                                                           , SYSDATETIME ())
                                                                  ELSE
                                                                  CASE WHEN pt.purchstatus IN ( 2, 3 )
                                                                        AND tr.ReceivedDate > '1/1/1900'
                                                                       THEN DATEDIFF (
                                                                                DAY
                                                                                , CAST(pt.deliverydate AS DATE)
                                                                                , tr.ReceivedDate)
                                                                       ELSE 0 END END END) > 0
                                               OR ISNULL (
                                                      (CASE WHEN pt.purchasetype = 3
                                                             AND CAST(pt.deliverydate AS DATE) > '1/1/1900'
                                                            THEN CASE WHEN pt.purchstatus = 1
                                                                      THEN CASE WHEN GETDATE () >= CAST(pt.deliverydate AS DATE)
                                                                                THEN 0 END
                                                                      ELSE
                                                                      CASE WHEN pt.purchstatus IN ( 2, 3 )
                                                                           THEN CASE WHEN tr.ReceivedDate <= CAST(pt.deliverydate AS DATE)
                                                                                     THEN 1 ELSE 0 END END END END)
                                                    , 0) = 1
                                             THEN 'DL'
                                             ELSE 'DOT' END END END END END AS VARCHAR(30)) AS OnTimeDeliveryStatus
             , pt.recid                                                                    AS _RecID
             , 1                                                                            AS _SourceID
          FROM {{ ref('purchtable') }} pt
          LEFT JOIN purchaseorder_factreceipt  tr
            ON tr.RecID = pt.recid
          LEFT JOIN purchaseorder_factline     tl
            ON tl.RecID = pt.recid
         WHERE pt.purchstatus <> 4;
),
purchaseorder_factlinecharge AS (
    SELECT pol._RecID                  AS RecID_PL
             , SUM (crg.IncludedCharge)    AS IncludedCharge
             , SUM (crg.AdditionalCharge)  AS AdditionalCharge
             , SUM (crg.NonBillableCharge) AS NonBillableCharge



          FROM {{ ref('purchaseorderlinecharge_f') }} crg
         INNER JOIN {{ ref('purchaseorderline_d') }}       pol
            ON pol.PurchaseOrderLineKey = crg.PurchaseOrderLineKey
         GROUP BY pol._RecID;
),
purchaseorder_factheadercharge AS (
    SELECT pt.recid                   AS RecID_PT
             , SUM (tlc.IncludedCharge)    AS IncludedCharge
             , SUM (tlc.AdditionalCharge)  AS AdditionalCharge
             , SUM (tlc.NonBillableCharge) AS NonBillableCharge



          FROM purchaseorder_factlinecharge         tlc
         INNER JOIN {{ ref('purchline') }}  pl
            ON pl.recid      = tlc.RecID_PL
         INNER JOIN {{ ref('purchtable') }} pt
            ON pt.dataareaid = pl.dataareaid
           AND pt.purchid     = pl.purchid
         GROUP BY pt.recid;
),
purchaseorder_factdetailmain AS (
    SELECT dpo.PurchaseOrderKey                                                                   AS PurchaseOrderKey
             , bg.BuyerGroupKey                                                                       AS BuyerGroupKey
             , de.EmployeeKey                                                                         AS BuyerKey
             , dc.CurrencyKey                                                                         AS CurrencyKey
             , cd.CashDiscountKey                                                                     AS CashDiscountKey
             , da.AddressKey                                                                          AS DeliveryAddressKey
             , dd3.DateKey                                                                            AS DeliveryDateActualKey
             , dd2.DateKey                                                                            AS DeliveryDateConfirmedKey
             , dd1.DateKey                                                                            AS DeliveryDateKey
             , ddm.DeliveryModeKey                                                                    AS DeliveryModeKey
             , ddt.DeliveryTermKey                                                                    AS DeliveryTermKey
             , dst.DocumentStateKey                                                                   AS DocumentStateKey
             , dds.DocumentStatusKey                                                                  AS DocumentStatusKey
             , dv2.VendorKey                                                                          AS InvoiceVendorKey
             , le.LegalEntityKey                                                                      AS LegalEntityKey
             , ot.OnTimeDeliveryStatusKey                                                             AS OnTimeDeliveryStatusKey
             , dd.DateKey                                                                             AS OrderDateKey
             , pm.PaymentModeKey                                                                      AS PaymentModeKey
             , pa.PaymentTermKey                                                                      AS PaymentTermKey
             , dps.PurchaseStatusKey                                                                  AS PurchaseStatusKey
             , dpt.PurchaseTypeKey                                                                    AS PurchaseTypeKey
             , de1.EmployeeKey                                                                        AS RequesterKey
             , drr.ReturnReasonKey                                                                    AS ReturnReasonKey
             , ds.InventorySiteKey                                                                    AS InventorySiteKey
             , tg.TaxGroupKey                                                                         AS TaxGroupKey
             , dv1.VendorKey                                                                          AS VendorKey
             , dw.WarehouseKey                                                                        AS WarehouseKey
             , ts.BaseAmount_TransCur * ISNULL (ex.ExchangeRate, 1)                                   AS BaseAmount
             , ISNULL ((thc.IncludedCharge + thc.AdditionalCharge ), 0) AS BillableCharge
             , ts.CashDiscountPercent                                                                 AS CashDiscountPercent
             , ts.DiscountAmount_TransCur * ISNULL (ex.ExchangeRate, 1)                               AS DiscountAmount
             , ts.NetAmount_TransCur * ISNULL (ex.ExchangeRate, 1)                                    AS NetAmount
             , ISNULL ((thc.NonBillableCharge ), 0)                  AS NonBillableCharge
             , ts.ReceivedAmount_TransCur * ISNULL (ex.ExchangeRate, 1)                               AS ReceivedAmount
             , ts.ReceivedNotInvoicedAmount_TransCur * ISNULL (ex.ExchangeRate, 1)                    AS ReceivedNotInvoicedAmount
             , ts.RemainingAmount_TransCur * ISNULL (ex.ExchangeRate, 1)                              AS RemainingAmount
             , ts.TotalAmount_TransCur * ISNULL (ex.ExchangeRate, 1)                                  AS TotalAmount
             , ts._RecID                                                                              AS _RecID
             , 1                                                                                      AS _SourceID

          FROM purchaseorder_factstage                        ts
         INNER JOIN {{ ref('purchaseorder_d') }}        dpo
            ON dpo._RecID              = ts._RecID
           AND dpo._SourceID           = 1
         INNER JOIN {{ ref('legalentity_d') }}          le
            ON le.LegalEntityID        = ts.LegalEntityID
          LEFT JOIN {{ ref('buyergroup_d') }}           bg
            ON bg.LegalEntityID        = ts.LegalEntityID
           AND bg.BuyerGroupID         = ts.BuyerGroupID
          LEFT JOIN {{ ref('address_d') }}              da
            ON da._RecID               = ts.DeliveryPostalAddress
           AND da._SourceID            = 1
          LEFT JOIN {{ ref('ontimedeliverystatus_d') }} ot
            ON ot.OnTimeDeliveryStatus = ts.OnTimeDeliveryStatus
          LEFT JOIN {{ ref('date_d') }}                 dd
            ON dd.Date                 = ts.OrderDate
          LEFT JOIN {{ ref('date_d') }}                 dd1
            ON dd1.Date                = ts.DeliveryDate
          LEFT JOIN {{ ref('date_d') }}                 dd2
            ON dd2.Date                = ts.DeliveryDateConfirmed
          LEFT JOIN {{ ref('date_d') }}                 dd3
            ON dd3.Date                = ts.DeliveryDateActual
          LEFT JOIN {{ ref('vendor_d') }}               dv1
            ON dv1.LegalEntityID       = ts.LegalEntityID
           AND dv1.VendorAccount       = ts.VendorAccount
          LEFT JOIN {{ ref('vendor_d') }}               dv2
            ON dv2.LegalEntityID       = ts.LegalEntityID
           AND dv2.VendorAccount       = ts.InvoiceAccount
          LEFT JOIN {{ ref('inventorysite_d') }}        ds
            ON ds.LegalEntityID        = ts.LegalEntityID
           AND ds.InventorySiteID      = ts.SiteID
          LEFT JOIN {{ ref('taxgroup_d') }}             tg
            ON tg.LegalEntityID        = ts.LegalEntityID
           AND tg.TaxGroupID           = ts.TaxGroupID
          LEFT JOIN {{ ref('warehouse_d') }}            dw
            ON dw.LegalEntityID        = ts.LegalEntityID
           AND dw.WarehouseID          = ts.WarehouseID
          LEFT JOIN {{ ref('employee_d') }}             de
            ON de._RecID               = ts.BuyerID
           AND de._SourceID            = 1
          LEFT JOIN {{ ref('employee_d') }}             de1
            ON de1._RecID              = ts.RequesterID
           AND de1._SourceID           = 1
          LEFT JOIN {{ ref('documentstatus_d') }}       dds
            ON dds.DocumentStatusID    = ts.DocumentStatusID
          LEFT JOIN {{ ref('documentstate_d') }}        dst
            ON dst.DocumentStateID     = ts.DocumentStateID
          LEFT JOIN {{ ref('purchasestatus_d') }}       dps
            ON dps.PurchaseStatusID    = ts.PurchaseStatusID
          LEFT JOIN {{ ref('purchasetype_d') }}         dpt
            ON dpt.PurchaseTypeID      = ts.PurchaseTypeID
          LEFT JOIN {{ ref('deliverymode_d') }}         ddm
            ON ddm.LegalEntityID       = ts.LegalEntityID
           AND ddm.DeliveryModeID      = ts.DeliveryModeID
          LEFT JOIN {{ ref('deliveryterm_d') }}         ddt
            ON ddt.LegalEntityID       = ts.LegalEntityID
           AND ddt.DeliveryTermID      = ts.DeliveryTermID
          LEFT JOIN {{ ref('paymentterm_d') }}          pa
            ON pa.LegalEntityID        = ts.LegalEntityID
           AND pa.PaymentTermID        = ts.PaymentTermID
          LEFT JOIN {{ ref('paymentmode_d') }}          pm
            ON pm.LegalEntityID        = ts.LegalEntityID
           AND pm.PaymentModeID        = ts.PaymentModeID
          LEFT JOIN {{ ref('currency_d') }}             dc
            ON dc.CurrencyID           = ts.CurrencyID
          LEFT JOIN {{ ref('returnreason_d') }}         drr
            ON drr.LegalEntityID       = ts.LegalEntityID
           AND drr.ReturnReasonID      = ts.ReturnReasonID
          LEFT JOIN {{ ref('exchangerate_f') }}    ex
            ON ex.ExchangeDateKey      = dd.DateKey
           AND ex.FromCurrencyID       = ts.CurrencyID
           AND ex.ToCurrencyID         = le.AccountingCurrencyID
           AND ex.ExchangeRateType     = le.TransExchangeRateType
          LEFT JOIN purchaseorder_factheadercharge            thc
            ON thc.RecID_PT            = ts._RecID
          LEFT JOIN {{ ref('cashdiscount_d') }}         cd
            ON cd.LegalEntityID        = ts.LegalEntityID
           AND cd.CashDiscountID       = ts.CashDiscountID;
)
SELECT 
           CURRENT_TIMESTAMP                                                                      AS _CreatedDate
         , CURRENT_TIMESTAMP                                                                      AS _ModifiedDate 
         , tdm.PurchaseOrderKey                                                                      AS PurchaseOrderKey
         , tdm.BuyerGroupKey                                                                         AS BuyerGroupKey
         , tdm.BuyerKey                                                                              AS BuyerKey
         , tdm.CurrencyKey                                                                           AS CurrencyKey
         , tdm.CashDiscountKey                                                                       AS CashDiscountKey
         , tdm.DeliveryAddressKey                                                                    AS DeliveryAddressKey
         , tdm.DeliveryDateActualKey                                                                 AS DeliveryDateActualKey
         , tdm.DeliveryDateConfirmedKey                                                              AS DeliveryDateConfirmedKey
         , tdm.DeliveryDateKey                                                                       AS DeliveryDateKey
         , tdm.DeliveryModeKey                                                                       AS DeliveryModeKey
         , tdm.DeliveryTermKey                                                                       AS DeliveryTermKey
         , tdm.DocumentStateKey                                                                      AS DocumentStateKey
         , tdm.DocumentStatusKey                                                                     AS DocumentStatusKey
         , tdm.InvoiceVendorKey                                                                      AS InvoiceVendorKey
         , tdm.LegalEntityKey                                                                        AS LegalEntityKey
         , tdm.OnTimeDeliveryStatusKey                                                               AS OnTimeDeliveryStatusKey
         , tdm.OrderDateKey                                                                          AS OrderDateKey
         , tdm.PaymentModeKey                                                                        AS PaymentModeKey
         , tdm.PaymentTermKey                                                                        AS PaymentTermKey
         , tdm.PurchaseStatusKey                                                                     AS PurchaseStatusKey
         , tdm.PurchaseTypeKey                                                                       AS PurchaseTypeKey
         , tdm.RequesterKey                                                                          AS RequesterKey
         , tdm.ReturnReasonKey                                                                       AS ReturnReasonKey
         , tdm.InventorySiteKey                                                                      AS InventorySiteKey
         , tdm.TaxGroupKey                                                                           AS TaxGroupKey
         , tdm.VendorKey                                                                             AS VendorKey
         , tdm.WarehouseKey                                                                          AS WarehouseKey
         , tdm.BaseAmount                                                                            AS BaseAmount
         , tdm.BillableCharge                                                                        AS BillableCharge
         , tdm.CashDiscountPercent                                                                   AS CashDiscountPercent
         , CASE WHEN tdm.DiscountAmount < 0 THEN tdm.DiscountAmount * -1 ELSE tdm.DiscountAmount END AS DiscountAmount
         , tdm.NetAmount                                                                             AS NetAmount
         , tdm.NonBillableCharge                                                                     AS NonBillableCharge
         , tdm.NetAmount + tdm.BillableCharge                                                        AS OrderedAmount
         , tdm.ReceivedAmount                                                                        AS ReceivedAmount
         , tdm.ReceivedNotInvoicedAmount                                                             AS ReceivedNotInvoicedAmount
         , tdm.RemainingAmount                                                                       AS RemainingAmount
         , tdm.TotalAmount                                                                           AS TotalAmount
         , tdm.BillableCharge + tdm.NonBillableCharge                                                AS TotalCharge
         , tdm._RecID                                                                                AS _RecID
         , 1                                                                                         AS _SourceID
      FROM purchaseorder_factdetailmain tdm;
