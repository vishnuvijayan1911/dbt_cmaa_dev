{{ config(materialized='table', tags=['silver'], alias='salesorder_fact') }}

-- Source file: cma/cma/layers/_base/_silver/salesorder_f/salesorder_f.py
-- Root method: SalesorderFact.salesorder_factdetail [SalesOrder_FactDetail]
-- Inlined methods: SalesorderFact.salesorder_factshipment [SalesOrder_FactShipment], SalesorderFact.salesorder_factline [SalesOrder_FactLine], SalesorderFact.salesorder_factstage [SalesOrder_FactStage], SalesorderFact.salesorder_factlinecharge [SalesOrder_FactLineCharge], SalesorderFact.salesorder_factheadercharge [SalesOrder_FactHeaderCharge], SalesorderFact.salesorder_factdetailmain [SalesOrder_FactDetailMain]
-- external_table_name: SalesOrder_FactDetail
-- schema_name: temp

WITH
salesorder_factshipment AS (
    SELECT sh.recid                                                                        AS RECID
             , MIN (cpst.deliverydate)                                                          AS ShippedDate
             , SUM (sl.salesprice * ISNULL (cpst.qty, 0) / ISNULL (NULLIF(sl.priceunit, 0), 1)) AS ShippedAmount_TransCur

          FROM {{ ref('salestable') }}                sh
         INNER JOIN {{ ref('salesline') }}            sl
            ON sl.dataareaid     = sh.dataareaid
           AND sl.salesid         = sh.salesid
         INNER JOIN {{ ref('custpackingsliptrans') }} cpst
            ON cpst.dataareaid   = sl.dataareaid
           AND cpst.inventtransid = sl.inventtransid
           AND cpst.inventdimid   = sl.inventdimid
         GROUP BY sh.recid;
),
salesorder_factline AS (
    SELECT sh.recid                                                                           AS RECID
             , SUM (sl.salesprice * sl.salesqty / ISNULL (NULLIF(sl.priceunit, 0), 1))             AS BaseAmount_TransCur
             , SUM (sl.cmatotalamount)                                                             AS TotalAmount_TransCur
             , SUM (sl.lineamount)                                                                 AS NetAmount_TransCur
             , SUM (sl.salesprice * sl.remainsalesphysical / ISNULL (NULLIF(sl.priceunit, 0), 1))  AS RemainingAmount_TransCur
             , SUM (sl.salesprice * sl.remainsalesfinancial / ISNULL (NULLIF(sl.priceunit, 0), 1)) AS ShippedNotInvoicedAmount_TransCur

          FROM {{ ref('salestable') }}     sh
         INNER JOIN {{ ref('salesline') }} sl
            ON sl.dataareaid = sh.dataareaid
           AND sl.salesid     = sh.salesid
         GROUP BY sh.recid;
),
salesorder_factstage AS (
    SELECT sh.recid                                  AS RECID
             , sh.dataareaid                             AS LegalEntityID
             , sh.currencycode                            AS CurrencyID
             , sh.dlvmode                                 AS DeliveryModeID
             , sh.dlvterm                                 AS DeliveryTermID
             , sh.dlvreason                               AS DeliveryReasonID
             , sh.payment                                 AS PaymentTermID
             , sh.taxgroup                                AS TaxGroupID
             , CAST(sh.createddatetime AS DATE)           AS OrderDate
             , CAST(sh.receiptdaterequested AS DATE)      AS ReceiptDateRequested
             , CAST(sh.receiptdateconfirmed AS DATE)      AS ReceiptDateConfirmed
             , sh.inventsiteid                            AS SiteID
             , sh.inventlocationid                        AS WarehouseID
             , sh.documentstatus                          AS DocumentStatusID
             , sh.salesstatus                             AS SalesStatusID
             , sh.salestype                               AS SalesTypeID
             , sh.workersalesresponsible                  AS SalesPersonID
             , sh.custaccount                             AS CustomerAccount
             , sh.custgroup                               AS CustomerGroupID
             , sh.invoiceaccount                          AS InvoiceAccount
             , sh.paymmode                                AS PaymentModeID
             , sh.returnstatus                            AS ReturnStatusID
             , sh.returnreasoncodeid                      AS ReturnReasonID
             , sh.workersalestaker                        AS SalesTaker
             , sh.cashdisc                                AS CashDiscountID
             , CAST(sh.cashdiscpercent AS NUMERIC(20, 6)) AS CashDiscountPercent
             , CAST(sh.enddisc AS MONEY)                  AS DiscountAmount_TransCur
             , sh.deliverypostaladdress                   AS DeliveryPostalAddress
             , tps.ShippedAmount_TransCur                 AS ShippedAmount_TransCur
             , tl.BaseAmount_TransCur                     AS BaseAmount_TransCur
             , tl.NetAmount_TransCur                      AS NetAmount_TransCur
             , tl.RemainingAmount_TransCur                AS RemainingAmount_TransCur
             , tl.ShippedNotInvoicedAmount_TransCur       AS ShippedNotInvoicedAmount_TransCur
             , tl.TotalAmount_TransCur                    AS TotalAmount_TransCur
             , tps.ShippedDate                            AS ShipDateActual
             , sh.modifieddatetime                       AS _SourceDate
             , CAST(CASE WHEN sh.salesstatus = 4
                         THEN NULL
                         WHEN sh.salesstatus NOT BETWEEN 1 AND 3
                         THEN NULL
                         WHEN sh.salestype <> 3
                         THEN NULL
                         WHEN (tps.ShippedDate IS NULL OR CAST(tps.ShippedDate AS DATE) <= '1/1/1900')
                          AND sh.salesstatus IN ( 2, 3 )
                         THEN NULL
                         WHEN (sh.deliverydate IS NULL OR CAST(sh.deliverydate AS DATE) <= '1/1/1900')
                          AND sh.salesstatus = 1
                         THEN 6 
                         WHEN (sh.deliverydate IS NULL OR CAST(sh.deliverydate AS DATE) <= '1/1/1900')
                          AND sh.salesstatus <> 1
                         THEN 5 
                         WHEN sh.salesstatus = 1
                          AND CAST(sh.deliverydate AS DATE) >= CAST(SYSDATETIME () AS DATE)
                         THEN 1 
                         WHEN sh.salesstatus = 1
                          AND CAST(sh.deliverydate AS DATE) < CAST(SYSDATETIME () AS DATE)
                         THEN 2 
                         WHEN sh.salesstatus IN ( 2, 3 )
                          AND CAST(sh.deliverydate AS DATE) >= CAST(tps.ShippedDate AS DATE)
                         THEN 4 
                         WHEN sh.salesstatus IN ( 2, 3 )
                          AND CAST(sh.deliverydate AS DATE) < CAST(tps.ShippedDate AS DATE)
                         THEN 3 
                         ELSE NULL END AS VARCHAR(30))    AS OnTimeShipStatus

          FROM {{ ref('salestable') }} sh
          LEFT JOIN salesorder_factshipment tps
            ON tps.RECID = sh.recid
          LEFT JOIN salesorder_factline     tl
            ON tl.RECID  = sh.recid
         WHERE sh.salesstatus <> 4;
),
salesorder_factlinecharge AS (
    SELECT sil._RecID                           AS RECID_SL
             , SUM (crg.IncludedCharge)             AS IncludedCharge
             , SUM (crg.IncludedCharge_TransCur)    AS IncludedCharge_TransCur
             , SUM (crg.AdditionalCharge)           AS AdditionalCharge
             , SUM (crg.AdditionalCharge_TransCur)  AS AdditionalCharge_TransCur
             , SUM (crg.NonBillableCharge)          AS NonBillableCharge
             , SUM (crg.NonBillableCharge_TransCur) AS NonBillableCharge_TransCur

          FROM {{ ref('salesorderlinecharge_f') }} crg
         INNER JOIN {{ ref('salesorderline_d') }}       sil
            ON sil.SalesOrderLineKey = crg.SalesOrderLineKey
         GROUP BY sil._RecID;
),
salesorder_factheadercharge AS (
    SELECT sh.recid                            AS RecID_SH
             , SUM (tlc.IncludedCharge)             AS IncludedCharge
             , SUM (tlc.IncludedCharge_TransCur)    AS IncludedCharge_TransCur
             , SUM (tlc.AdditionalCharge)           AS AdditionalCharge
             , SUM (tlc.AdditionalCharge_TransCur)  AS AdditionalCharge_TransCur
             , SUM (tlc.NonBillableCharge)          AS NonBillableCharge
             , SUM (tlc.NonBillableCharge_TransCur) AS NonBillableCharge_TransCur

          FROM {{ ref('salestable') }}     sh
         INNER JOIN {{ ref('salesline') }} sl
            ON sl.dataareaid = sh.dataareaid
           AND sl.salesid     = sh.salesid
         INNER JOIN salesorder_factlinecharge   tlc
            ON tlc.RECID_SL   = sl.recid
         GROUP BY sh.recid;
),
salesorder_factdetailmain AS (
    SELECT dso.SalesOrderKey                                                         AS SalesOrderKey
             , cd.CashDiscountKey                                                        AS CashDiscountKey
             , dc.CurrencyKey                                                            AS CurrencyKey
             , dc1.CustomerKey                                                           AS CustomerKey
             , dcg.CustomerGroupKey                                                      AS CustomerGroupKey
             , ddm.DeliveryModeKey                                                       AS DeliveryModeKey
             , ddt.DeliveryTermKey                                                       AS DeliveryTermKey
             , ddr.DeliveryReasonKey                                                     AS DeliveryReasonKey
             , dds.DocumentStatusKey                                                     AS DocumentStatusKey
             , dc2.CustomerKey                                                           AS InvoiceCustomerKey
             , le.LegalEntityKey                                                         AS LegalEntityKey
             , ot.OnTimeShipStatusKey                                                    AS OnTimeShipStatusKey
             , dd.DateKey                                                                AS OrderDateKey
             , pm.PaymentModeKey                                                         AS PaymentModeKey
             , dpa.PaymentTermKey                                                        AS PaymentTermKey
             , dd1.DateKey                                                               AS ReceiptDateConfirmedKey
             , dd2.DateKey                                                               AS ReceiptDateRequestedKey
             , drs.ReturnStatusKey                                                       AS ReturnStatusKey
             , rsi.ReturnReasonKey                                                       AS ReturnReasonKey
             , dsp.SalesPersonKey                                                        AS SalesPersonKey
             , de.EmployeeKey                                                            AS SalesTakerKey
             , dss.SalesStatusKey                                                        AS SalesStatusKey
             , dst.SalesTypeKey                                                          AS SalesTypeKey
             , dd3.DateKey                                                               AS ShipDateActualKey
             , ds.InventorySiteKey                                                       AS InventorySiteKey
             , tg.TaxGroupKey                                                            AS TaxGroupKey
             , dw.WarehouseKey                                                           AS WarehouseKey
             , ts.BaseAmount_TransCur * ISNULL (ex.ExchangeRate, 1)                      AS BaseAmount
             , ts.BaseAmount_TransCur
             , ISNULL ((thc.IncludedCharge + thc.AdditionalCharge), 0)                   AS BillableCharge
             , ISNULL ((thc.IncludedCharge_TransCur + thc.AdditionalCharge_TransCur), 0) AS BillableCharge_TransCur
             , ts.CashDiscountPercent                                                    AS CashDiscountPercent
             , ts.DiscountAmount_TransCur * ISNULL (ex.ExchangeRate, 1)                  AS DiscountAmount
             , ts.DiscountAmount_TransCur
             , ts.NetAmount_TransCur * ISNULL (ex.ExchangeRate, 1)                       AS NetAmount
             , ts.NetAmount_TransCur
             , ISNULL (thc.NonBillableCharge, 0)                                         AS NonBillableCharge
             , thc.NonBillableCharge_TransCur
             , ts.RemainingAmount_TransCur * ISNULL (ex.ExchangeRate, 1)                 AS RemainingAmount
             , ts.RemainingAmount_TransCur
             , ts.ShippedAmount_TransCur * ISNULL (ex.ExchangeRate, 1)                   AS ShippedAmount
             , ts.ShippedAmount_TransCur
             , ts.ShippedNotInvoicedAmount_TransCur * ISNULL (ex.ExchangeRate, 1)        AS ShippedNotInvoicedAmount
             , ts.ShippedNotInvoicedAmount_TransCur
             , ts.TotalAmount_TransCur * ISNULL (ex.ExchangeRate, 1)                     AS TotalAmount
             , ts.TotalAmount_TransCur
             , ts.SalesStatusID                                                          AS SalesStatusID
             , ts._SourceDate
             , 1                                                                         AS _SourceID
             , ts.RECID                                                                  AS _RecID
          FROM salesorder_factstage                  ts
         INNER JOIN {{ ref('salesorder_d') }}        dso
            ON dso._RecID           = ts.RECID
           AND dso._SourceID        = 1
         INNER JOIN {{ ref('legalentity_d') }}       le
            ON le.LegalEntityID     = ts.LegalEntityID
          LEFT JOIN {{ ref('address_d') }}           da
            ON da._RecID            = ts.DeliveryPostalAddress
           AND da._SourceID         = 1
          LEFT JOIN {{ ref('ontimeshipstatus_d') }}  ot
            ON ot.OnTimeShipStatus  = ts.OnTimeShipStatus
          LEFT JOIN {{ ref('customer_d') }}          dc1
            ON dc1.LegalEntityID    = ts.LegalEntityID
           AND dc1.CustomerAccount  = ts.CustomerAccount
          LEFT JOIN {{ ref('customer_d') }}          dc2
            ON dc2.LegalEntityID    = ts.LegalEntityID
           AND dc2.CustomerAccount  = ts.InvoiceAccount
          LEFT JOIN {{ ref('customergroup_d') }}     dcg
            ON dcg.LegalEntityID    = ts.LegalEntityID
           AND dcg.CustomerGroupID  = ts.CustomerGroupID
          LEFT JOIN {{ ref('date_d') }}              dd
            ON dd.Date              = ts.OrderDate
          LEFT JOIN {{ ref('date_d') }}              dd1
            ON dd1.Date             = ts.ReceiptDateConfirmed
          LEFT JOIN {{ ref('date_d') }}              dd2
            ON dd2.Date             = ts.ReceiptDateRequested
          LEFT JOIN {{ ref('date_d') }}              dd3
            ON dd3.Date             = ts.ShipDateActual
          LEFT JOIN {{ ref('inventorysite_d') }}     ds
            ON ds.LegalEntityID     = ts.LegalEntityID
           AND ds.InventorySiteID   = ts.SiteID
          LEFT JOIN {{ ref('taxgroup_d') }}          tg
            ON tg.LegalEntityID     = ts.LegalEntityID
           AND tg.TaxGroupID        = ts.TaxGroupID
          LEFT JOIN {{ ref('warehouse_d') }}         dw
            ON dw.LegalEntityID     = ts.LegalEntityID
           AND dw.WarehouseID       = ts.WarehouseID
          LEFT JOIN {{ ref('salesperson_d') }}       dsp
            ON dsp._RecID           = ts.SalesPersonID
           AND dsp._SourceID        = 1
          LEFT JOIN {{ ref('employee_d') }}          de
            ON de._RecID            = ts.SalesTaker
           AND de._SourceID         = 1
          LEFT JOIN {{ ref('documentstatus_d') }}    dds
            ON dds.DocumentStatusID = ts.DocumentStatusID
          LEFT JOIN {{ ref('salesstatus_d') }}       dss
            ON dss.SalesStatusID    = ts.SalesStatusID
          LEFT JOIN {{ ref('salestype_d') }}         dst
            ON dst.SalesTypeID      = ts.SalesTypeID
          LEFT JOIN {{ ref('deliverymode_d') }}      ddm
            ON ddm.LegalEntityID    = ts.LegalEntityID
           AND ddm.DeliveryModeID   = ts.DeliveryModeID
          LEFT JOIN {{ ref('deliveryterm_d') }}      ddt
            ON ddt.LegalEntityID    = ts.LegalEntityID
           AND ddt.DeliveryTermID   = ts.DeliveryTermID
          LEFT JOIN {{ ref('paymentterm_d') }}       dpa
            ON dpa.LegalEntityID    = ts.LegalEntityID
           AND dpa.PaymentTermID    = ts.PaymentTermID
          LEFT JOIN {{ ref('returnstatus_d') }}      drs
            ON drs.ReturnStatusID   = ts.ReturnStatusID
          LEFT JOIN {{ ref('paymentmode_d') }}       pm
            ON pm.LegalEntityID     = ts.LegalEntityID
           AND pm.PaymentModeID     = ts.PaymentModeID
          LEFT JOIN {{ ref('currency_d') }}          dc
            ON dc.CurrencyID        = ts.CurrencyID
          LEFT JOIN {{ ref('returnreason_d') }}      rsi
            ON rsi.LegalEntityID    = ts.LegalEntityID
           AND rsi.ReturnReasonID   = ts.ReturnReasonID
          LEFT JOIN {{ ref('exchangerate_f') }} ex
            ON ex.ExchangeDateKey   = dd.DateKey
           AND ex.FromCurrencyID    = ts.CurrencyID
           AND ex.ToCurrencyID      = le.AccountingCurrencyID
           AND ex.ExchangeRateType  = le.TransExchangeRateType
          LEFT JOIN salesorder_factheadercharge         thc
            ON thc.RecID_SH         = ts.RECID
          LEFT JOIN {{ ref('cashdiscount_d') }}      cd
            ON cd.LegalEntityID     = ts.LegalEntityID
           AND cd.CashDiscountID    = ts.CashDiscountID
          LEFT JOIN {{ ref('deliveryreason_d') }}    ddr
            ON ddr.LegalEntityID    = ts.LegalEntityID
           AND ddr.DeliveryReasonID = ts.DeliveryReasonID;
)
SELECT 
         , tdm.SalesOrderKey                                                                                    AS SalesOrderKey
         , tdm.CashDiscountKey                                                                                  AS CashDiscountKey
         , tdm.CurrencyKey                                                                                      AS CurrencyKey
         , tdm.CustomerGroupKey                                                                                 AS CustomerGroupKey
         , tdm.CustomerKey                                                                                      AS CustomerKey
         , tdm.DeliveryModeKey                                                                                  AS DeliveryModeKey
         , tdm.DeliveryTermKey                                                                                  AS DeliveryTermKey
         , tdm.DeliveryReasonKey                                                                                AS DeliveryReasonKey
         , tdm.DocumentStatusKey                                                                                AS DocumentStatusKey
         , tdm.InvoiceCustomerKey                                                                               AS InvoiceCustomerKey
         , tdm.LegalEntityKey                                                                                   AS LegalEntityKey
         , tdm.OnTimeShipStatusKey                                                                              AS OnTimeShipStatusKey
         , tdm.OrderDateKey                                                                                     AS OrderDateKey
         , tdm.PaymentModeKey                                                                                   AS PaymentModeKey
         , tdm.PaymentTermKey                                                                                   AS PaymentTermKey
         , tdm.ReceiptDateConfirmedKey                                                                          AS ReceiptDateConfirmedKey
         , tdm.ReceiptDateRequestedKey                                                                          AS ReceiptDateRequestedKey
         , tdm.ReturnStatusKey                                                                                  AS ReturnStatusKey
         , tdm.ReturnReasonKey                                                                                  AS ReturnReasonKey
         , tdm.SalesPersonKey                                                                                   AS SalesPersonKey
         , tdm.SalesStatusKey                                                                                   AS SalesStatusKey
         , tdm.SalesTakerKey                                                                                    AS SalesTakerKey
         , tdm.SalesTypeKey                                                                                     AS SalesTypeKey
         , tdm.ShipDateActualKey                                                                                AS ShipDateActualKey
         , tdm.InventorySiteKey                                                                                 AS InventorySiteKey
         , tdm.TaxGroupKey                                                                                      AS TaxGroupKey
         , tdm.WarehouseKey                                                                                     AS WarehouseKey
         , tdm.BaseAmount                                                                                       AS BaseAmount
         , tdm.BaseAmount_TransCur
         , tdm.BillableCharge                                                                                   AS BillableCharge
         , tdm.BillableCharge_TransCur
         , tdm.CashDiscountPercent                                                                              AS CashDiscountPercent
         , tdm.DiscountAmount                                                                                   AS DiscountAmount
         , tdm.DiscountAmount_TransCur
         , tdm.NetAmount                                                                                        AS NetAmount
         , tdm.NetAmount_TransCur
         , tdm.NonBillableCharge                                                                                AS NonBillableCharge
         , tdm.NonBillableCharge_TransCur
         , CASE WHEN tdm.SalesStatusID = 1 THEN tdm.NetAmount + tdm.BillableCharge ELSE 0 END                   AS OpenAmount
         , CASE WHEN tdm.SalesStatusID = 1 THEN tdm.NetAmount_TransCur + tdm.BillableCharge_TransCur ELSE 0 END AS OpenAmount_TransCur
         , tdm.NetAmount + tdm.BillableCharge                                                                   AS OrderedAmount
         , tdm.NetAmount_TransCur + tdm.BillableCharge_TransCur                                                 AS OrderedAmount_TransCur
         , tdm.RemainingAmount                                                                                  AS RemainingAmount
         , tdm.RemainingAmount_TransCur
         , tdm.ShippedAmount                                                                                    AS ShippedAmount
         , tdm.ShippedAmount_TransCur
         , tdm.ShippedNotInvoicedAmount                                                                         AS ShippedNotInvoicedAmount
         , tdm.ShippedNotInvoicedAmount_TransCur
         , TotalAmount                                                                                          AS TotalAmount
         , tdm.TotalAmount_TransCur
         , tdm.BillableCharge + tdm.NonBillableCharge                                                           AS TotalCharge
         , tdm.BillableCharge_TransCur + tdm.NonBillableCharge_TransCur                                         AS TotalCharge_TransCur
         , tdm._RecID                                                                                           AS _RecID
         , tdm._SourceID                                                                                        AS _SourceID
         , tdm._SourceDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM salesorder_factdetailmain tdm;
