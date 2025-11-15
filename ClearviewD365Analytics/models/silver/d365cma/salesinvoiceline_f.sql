{{ config(materialized='table', tags=['silver'], alias='salesinvoiceline_fact') }}

-- Source file: cma/cma/layers/_base/_silver/salesinvoiceline_f/salesinvoiceline_f.py
-- Root method: SalesinvoicelineFact.get_detail_query [SalesInvoiceLine_FactDetail]
-- Inlined methods: SalesinvoicelineFact.get_charge_query [SalesInvoiceLine_FactCharge], SalesinvoicelineFact.get_packing_slip_query [SalesInvoiceLine_FactPackingSlip], SalesinvoicelineFact.get_tax_query [SalesInvoiceLine_FactTax], SalesinvoicelineFact.get_inventory_cost_1_query [SalesInvoiceLine_FactInventoryCost1], SalesinvoicelineFact.get_inventory_cost_query [SalesInvoiceLine_FactInventoryCost], SalesinvoicelineFact.get_stage_query [SalesInvoiceLine_FactStage], SalesinvoicelineFact.get_main_query [SalesInvoiceLine_FactMain], SalesinvoicelineFact.get_line_query [SalesInvoiceLine_FactLine]
-- external_table_name: SalesInvoiceLine_FactDetail
-- schema_name: temp

WITH
salesinvoiceline_factcharge AS (
    SELECT sil.SalesInvoiceLineKey            AS SalesInvoiceLineKey
        , SUM(crg.IncludedCharge)             AS IncludedCharge
        , SUM(crg.IncludedCharge_TransCur)    AS IncludedCharge_TransCur
        , SUM(crg.AdditionalCharge)           AS AdditionalCharge
        , SUM(crg.AdditionalCharge_TransCur)  AS AdditionalCharge_TransCur
        , SUM(crg.NonBillableCharge)          AS NonBillableCharge
        , SUM(crg.NonBillableCharge_TransCur) AS NonBillableCharge_TransCur
        , SUM(crg.TaxAmount)                  AS TaxAmount
        , SUM(crg.TaxAmount_TransCur)         AS TaxAmount_TransCur
      FROM silver.cma_SalesInvoiceLineCharge_Fact crg
    INNER JOIN silver.cma_SalesInvoiceLine       sil
        ON sil.SalesInvoiceLineKey = crg.SalesInvoiceLineKey
    GROUP BY sil.SalesInvoiceLineKey;
),
salesinvoiceline_factpackingslip AS (
    SELECT cit.recid           AS RECID
        , MIN(ps.deliverydate) AS SHIPDATE
      FROM {{ ref('custinvoicejour') }}           cij    
      LEFT JOIN {{ ref('custinvoicetrans') }}     cit
        ON cij.dataareaid          = cit.dataareaid
      AND cij.salesid             = cit.salesid
      AND cij.invoiceid           = cit.invoiceid
      AND cij.invoicedate         = cit.invoicedate
      AND cij.numbersequencegroup = cit.numbersequencegroup
      AND (cij.recid              = cit.parentrecid OR cij.salestype <> 0)
    INNER JOIN {{ ref('custpackingsliptrans') }} ps
        ON ps.dataareaid           = cit.dataareaid
      AND ps.inventtransid        = cit.inventtransid
      AND ps.itemid               = cit.itemid
    GROUP BY cit.recid;
),
salesinvoiceline_facttax AS (
    SELECT tt.sourcerecid            AS RECID
        , SUM(tt.taxamount * -1)     AS TaxAmount
        , SUM(tt.taxamountcur * -1)  AS TaxAmountCur
      FROM {{ ref('custinvoicejour') }}       cij
      LEFT JOIN {{ ref('custinvoicetrans') }} cit
        ON cij.dataareaid          = cit.dataareaid
      AND cij.salesid             = cit.salesid
      AND cij.invoiceid           = cit.invoiceid
      AND cij.invoicedate         = cit.invoicedate
      AND cij.numbersequencegroup = cit.numbersequencegroup
      AND (cij.recid              = cit.parentrecid OR cij.salestype <> 0)
    INNER JOIN {{ ref('taxtrans') }}         tt
        ON tt.sourcerecid          = cit.recid
    INNER JOIN {{ ref('sqldictionary') }}    sd
        ON sd.tabid              = tt.sourcetableid
      AND sd.name                 = 'CustInvoiceTrans'
    GROUP BY tt.sourcerecid;
),
salesinvoiceline_factinventorycost1 AS (
    SELECT cit.recid    AS RECID
        , cit.invoiceid AS INVOICEID
        , ito.recid     AS RECID_ITO
      FROM {{ ref('custinvoicejour') }}        cij
      LEFT JOIN {{ ref('custinvoicetrans') }}  cit
        ON cij.dataareaid          = cit.dataareaid
      AND cij.salesid             = cit.salesid
      AND cij.invoiceid           = cit.invoiceid
      AND cij.invoicedate         = cit.invoicedate
      AND cij.numbersequencegroup = cit.numbersequencegroup
      AND (cij.recid              = cit.parentrecid OR cij.salestype <> 0)
    INNER JOIN {{ ref('inventtransorigin') }} ito
        ON ito.dataareaid          = cit.dataareaid
      AND ito.inventtransid       = cit.inventtransid
      AND ito.itemid              = cit.itemid;
),
salesinvoiceline_factinventorycost AS (
    SELECT ts.RECID                                               AS RECID
        , ts.RECID_ITO                                            AS RECID_ITO
        , SUM(it.costamountposted + it.costamountadjustment) * -1 AS COSTAMOUNT
      FROM salesinvoiceline_factinventorycost1      ts
    INNER JOIN {{ ref('inventtrans') }} it
        ON it.inventtransorigin = ts.RECID_ITO
      AND it.invoiceid         = ts.INVOICEID
    GROUP BY ts.RECID
            , ts.RECID_ITO;
),
salesinvoiceline_factstage AS (
    SELECT sl.recid                                                                                                    AS RecID_SL
      , cij.invoicedate                                                                                                AS InvoiceDate
      , cij.orderaccount                                                                                               AS OrderAccount
      , cij.refnum                                                                                                     AS ReferenceTypeID
      , cij.currencycode                                                                                               AS CurrencyID
      , CASE WHEN sh.salesid IS NULL THEN ct.dlvmode ELSE sh.dlvmode END                                               AS DeliveryModeID
      , CASE WHEN sh.salesid IS NULL THEN civ.dlvterm ELSE sh.dlvterm END                                              AS DeliveryTermID
      , CASE WHEN sh.salesid IS NULL THEN cij.payment ELSE sh.payment END                                              AS PaymentTermID
      , CASE WHEN sh.salesid IS NULL THEN civ.paymmode ELSE sh.paymmode END                                            AS PaymentModeID
      , cit.cmapriceuom                                                                                          AS PricingUnit
      , sh.taxgroup                                                                                                    AS TaxGroupID
      , cij.ledgervoucher                                                                                              AS VoucherID
      , cij.invoiceaccount                                                                                             AS InvoiceAccount
      , cij.cashdisccode                                                                                               AS CashDiscountID
      , cij.salestype                                                                                                  AS SalesTypeID
      , id.inventsiteid                                                                                                AS SiteID
      , id.inventlocationid                                                                                            AS WarehouseID
      , id.inventsizeid                                                                                                AS ProductWidth
      , id.inventcolorid                                                                                               AS ProductLength
      , id.inventstyleid                                                                                               AS ProductColor
      , id.configid                                                                                                    AS ProductConfig
      , sh.workersalesresponsible                                                                                      AS WorkerSalesResponsible
      , tic.RECID_ITO                                                                                                  AS RecID_ITO
      , cit.deliverypostaladdress                                                                                      AS DeliveryPostalAddress
      , ISNULL (cit.defaultdimension, cij.defaultdimension)                                                            AS DefaultDimension
      , cit.itemid                                                                                                     AS ItemID
      , cij.dataareaid                                                                                                 AS LegalEntityID
      , cit.salesunit                                                                                                  AS SalesUOM
      , CASE WHEN ISNULL (cit.qty, 0) = 0
                OR cij.refnum <> 0
              THEN cit.lineamount
              ELSE (cit.salesprice * cit.qty / ISNULL (NULLIF(cit.priceunit, 0), 1)) END * cij.exchrate / 100           AS BaseAmount
      , CASE WHEN ISNULL (cit.qty, 0) = 0
                OR cij.refnum <> 0
              THEN cit.lineamount
              ELSE (cit.salesprice * cit.qty / ISNULL (NULLIF(cit.priceunit, 0), 1)) END                                AS BaseAmount_TransCur
      , ISNULL (tic.COSTAMOUNT, 0)                                                                                      AS CostAmount
      , ISNULL (tic.COSTAMOUNT, 0) * 100 / ISNULL (NULLIF(cij.exchrate, 0), 1)                                          AS CostAmount_TransCur
      , ISNULL (cit.lineamountmst, cij.invoiceamountmst)                                                                AS NetAmount
      , ISNULL (cit.lineamount, cij.invoiceamount)                                                                      AS NetAmount_TransCur
      , CASE WHEN cit.cmanetamount IS NULL
              THEN cij.invoiceamountmst
              ELSE
              CASE WHEN cit.cmanetamount <> 0 THEN cit.cmanetamount * cij.exchrate / 100 ELSE cit.lineamountmst END END AS InvoiceSalesAmount
      , CASE WHEN cit.cmanetamount IS NULL
              THEN cij.invoiceamount
              ELSE CASE WHEN cit.cmanetamount <> 0 THEN cit.cmanetamount ELSE cit.lineamount END END                     AS InvoiceSalesAmount_TransCur
      , cit.qty                                                                                                          AS InvoiceQuantity_SalesUOM
      , cit.inventqty                                                                                                    AS InvoiceQuantity
      , CASE WHEN cit.taxamount IS NULL
              THEN NULL
              ELSE CASE WHEN cij.refnum = 0 THEN cit.taxamount * cij.exchrate / 100 ELSE tt.taxamount END END            AS TaxAmount
      , CASE WHEN cij.refnum = 0 THEN ISNULL (cit.taxamount, cij.sumtax) ELSE tt.taxamountcur END                        AS TaxAmount_TransCur
      , cit.salesprice * cij.exchrate / 100                                                                              AS BaseUnitPrice
      , cit.salesprice                                                                                                   AS BaseUnitPrice_TransCur
      , CASE WHEN cit.cmanetprice <> 0 THEN cit.cmanetprice ELSE cit.salesprice END * cij.exchrate / 100                 AS TotalUnitPrice
      , CASE WHEN cit.cmanetprice <> 0 THEN cit.cmanetprice ELSE cit.salesprice END                                      AS TotalUnitPrice_TransCur
      , cit.priceunit                                                                                                    AS PriceUnit
      , cij.returnstatus                                                                                                 AS ReturnStatus
      , cij.returnreasoncodeid                                                                                           AS ReturnReasonID
      , CAST(cit.createddatetime AS DATE)                                                                                AS CreatedDate
      , cij.duedate                                                                                                      AS DueDate
      , ps.shipdate                                                                                                      AS ShipDate
      , sh.workersalestaker                                                                                              AS SalesTaker
      , sl.salescategory                                                                                                 AS RecID_SC
      , cit.modifieddatetime                                                                                             AS _SourceDate
      , 1                                                                                                                AS _SourceID
      , cij.recid                                                                                                        AS _RecID1
      , ISNULL (cit.recid, 0)                                                                                            AS _RecID2
    FROM {{ ref('custinvoicejour') }}       cij
    LEFT JOIN {{ ref('custinvoicetrans') }} cit
      ON cij.dataareaid          = cit.dataareaid
    AND cij.salesid             = cit.salesid
    AND cij.invoiceid           = cit.invoiceid
    AND cij.invoicedate         = cit.invoicedate
    AND cij.numbersequencegroup = cit.numbersequencegroup
    AND (cij.recid              = cit.parentrecid OR cij.salestype <> 0)
    LEFT JOIN {{ ref('inventdim') }}        id
      ON cit.dataareaid          = id.dataareaid
    AND cit.inventdimid         = id.inventdimid
    LEFT JOIN {{ ref('salestable') }}        sh
      ON sh.dataareaid           = cit.dataareaid
    AND sh.salesid              = cit.origsalesid
    LEFT JOIN {{ ref('salesline') }}        sl
      ON sl.dataareaid           = cit.dataareaid
    AND sl.inventtransid        = cit.inventtransid
    AND sl.itemid               = cit.itemid
    LEFT JOIN {{ ref('custinvoicetable') }} civ
      ON civ.dataareaid          = cij.dataareaid
    AND civ.invoiceid           = cij.invoiceid
    AND civ.invoicedate         = cij.invoicedate
    AND civ.numbersequencegroup = cij.numbersequencegroup
    LEFT JOIN {{ ref('custtable') }}         ct
      ON ct.dataareaid           = civ.dataareaid
    AND ct.accountnum           = civ.orderaccount
    LEFT JOIN salesinvoiceline_factinventorycost       tic
      ON tic.RECID               = cit.recid
    LEFT JOIN salesinvoiceline_facttax                 tt
      ON tt.recid                = cit.recid
    LEFT JOIN salesinvoiceline_factpackingslip         ps
      ON ps.recid                = cit.recid;
),
salesinvoiceline_factmain AS (
    SELECT dcil.SalesInvoiceLineKey                                               AS SalesInvoiceLineKey
        , le.LegalEntityKey                                                       AS LegalEntityKey
        , cd.CashDiscountKey                                                      AS CashDiscountKey
        , dc.CustomerKey                                                          AS CustomerKey
        , cc.CurrencyKey                                                          AS CurrencyKey
        , dt.DeliveryTermKey                                                      AS DeliveryTermKey
        , dm.DeliveryModeKey                                                      AS DeliveryModeKey
        , dca1.FinancialKey                                                       AS FinancialKey
        , si.SalesInvoiceKey                                                      AS SalesInvoiceKey
        , ivt.InvoiceTypeKey                                                      AS InvoiceTypeKey
        , pam.PaymentModeKey                                                      AS PaymentModeKey
        , pyt.PaymentTermKey                                                      AS PaymentTermKey
        , tg.TaxGroupKey                                                          AS TaxGroupKey
        , su.UOMKey                                                               AS SalesUOMKey
        , pu.UOMKey                                                               AS PricingUOMKey
        , vou.VoucherKey                                                          AS VoucherKey
        , da.AddressKey                                                           AS DeliveryAddressKey
        , dc2.CustomerKey                                                         AS InvoiceCustomerKey
        , dd1.DateKey                                                             AS InvoiceDateKey
        , dd2.DateKey                                                             AS CreatedDateKey
        , dd3.DateKey                                                             AS DueDateKey
        , drr.ReturnReasonKey                                                     AS ReturnReasonkey
        , drs.ReturnStatusKey                                                     AS ReturnStatusKey
        , dsc.SalesCategoryKey                                                    AS SalesCategoryKey
        , dd4.DateKey                                                             AS ShipDateKey
        , dp.InventorySiteKey                                                     AS InventorySiteKey
        , it.lotkey                                                               AS LotKey
        , ISNULL (dr.ProductKey, -1)                                              AS ProductKey
        , de2.EmployeeKey                                                         AS SalesTakerKey
        , sit.SalesTypeKey                                                        AS SalesTypeKey
        , dsil.SalesOrderLineKey                                                  AS SalesOrderLineKey
        , dsp.SalesPersonKey                                                      AS SalesPersonKey
        , dw.WarehouseKey                                                         AS WarehouseKey
        , ts.LegalEntityID                                                        AS LegalEntityID
        , dr.ProductID                                                            AS ProductID
        , dr.InventoryUOM                                                         AS InventoryUnit
        , ts.SalesUOM                                                             AS SalesUOM
        , ts.BaseAmount                                                           AS BaseAmount
        , ts.BaseAmount_TransCur                                                  AS BaseAmount_TransCur
        , ca.IncludedCharge                                                       AS IncludedCharge
        , ca.IncludedCharge_TransCur                                              AS IncludedCharge_TransCur
        , ca.AdditionalCharge                                                     AS AdditionalCharge
        , ca.AdditionalCharge_TransCur                                            AS AdditionalCharge_TransCur
        , ca.NonBillableCharge                                                    AS NonBillableCharge
        , ca.NonBillableCharge_TransCur                                           AS NonBillableCharge_TransCur
        , ISNULL ((ca.IncludedCharge + ca.AdditionalCharge), 0)                   AS CustomerCharge
        , ISNULL ((ca.IncludedCharge_TransCur + ca.AdditionalCharge_TransCur), 0) AS CustomerCharge_TransCur
        , ts.CostAmount                                                           AS CostAmount
        , ts.CostAmount_TransCur                                                  AS CostAmount_TransCur
        , CASE WHEN ts.ReferenceTypeID = 0
                THEN ROUND (
                        ((CASE WHEN CAST(ts.BaseUnitPrice AS Numeric(32,6)) = 0
                                THEN CASE WHEN CAST(ts.InvoiceSalesAmount AS NUMERIC(32,6)) = 0
                                          THEN 0
                                          ELSE ((CAST(ts.NetAmount AS NUMERIC(32,16))) / ISNULL (NULLIF(CAST(ts.InvoiceQuantity_SalesUOM AS NUMERIC(32,16)), 0), 1)) END
                                ELSE CAST(ts.BaseUnitPrice AS Numeric(32,6)) / (ISNULL (NULLIF(CAST(ts.PriceUnit AS NUMERIC(32,16)), 0), 1)) END)
                          * (CASE WHEN CAST(InvoiceQuantity_SalesUOM AS NUMERIC(32,16)) = 0 THEN 1 ELSE CAST(InvoiceQuantity_SalesUOM AS NUMERIC(32,16)) END))
                        - CAST(NetAmount AS NUMERIC(32,16))
                      , 2)
                ELSE 0 END                                                         AS DiscountAmount

        , CASE WHEN ts.ReferenceTypeID = 0
                THEN ROUND (
                        ((CASE WHEN CAST(ts.BaseUnitPrice_TransCur AS NUMERIC(32,6) )= 0
                                THEN CASE WHEN CAST(ts.InvoiceSalesAmount_TransCur AS NUMERIC(32,6) ) = 0
                                          THEN 0
                                          ELSE
                        ((CAST(ts.NetAmount_TransCur AS NUMERIC(32,16))) / ISNULL (NULLIF(CAST(ts.InvoiceQuantity_SalesUOM AS NUMERIC(32,16)), 0), 1)) END
                                ELSE CAST(ts.BaseUnitPrice_TransCur AS NUMERIC(32,6)) / (ISNULL (NULLIF(CAST(ts.PriceUnit AS NUMERIC(32,16)), 0), 1)) END)
                          * (CASE WHEN CAST(ts.InvoiceQuantity_SalesUOM AS NUMERIC(32,16)) = 0 THEN 1 ELSE CAST(ts.InvoiceQuantity_SalesUOM AS NUMERIC(32,16) ) END))
                        - CAST(ts.NetAmount_TransCur AS NUMERIC(32,16) )
                      , 2)
                ELSE 0 END                                                        AS DiscountAmount_TransCur
        , ts.InvoiceQuantity_SalesUOM                                             AS InvoiceQuantity_SalesUOM
        , ts.InvoiceQuantity                                                      AS InvoiceQuantity
        , ts.NetAmount                                                            AS NetAmount
        , ts.NetAmount_TransCur                                                   AS NetAmount_TransCur
        , ts.InvoiceSalesAmount                                                   AS InvoiceSalesAmount
        , ts.InvoiceSalesAmount_TransCur                                          AS InvoiceSalesAmount_TransCur
        , ts.TaxAmount + ISNULL (ca.TaxAmount, 0)                                 AS TaxAmount
        , ts.TaxAmount_TransCur + ISNULL (ca.TaxAmount_TransCur, 0)               AS TaxAmount_TransCur
        , ts.BaseUnitPrice                                                        AS BaseUnitPrice
        , ts.BaseUnitPrice_TransCur                                               AS BaseUnitPrice_TransCur
        , ts.TotalUnitPrice                                                       AS TotalUnitPrice
        , ts.TotalUnitPrice_TransCur                                              AS TotalUnitPrice_TransCur
        , ts.PriceUnit                                                            AS PriceUnit
        , le.AccountingCurrencyID                                                 AS AccountingCurrencyID
        , le.TransExchangeRateType                                                AS TransExchangeRateType
        , ts._SourceDate
        , ts._RecID1                                                              AS _RecID1
        , ts._RecID2                                                              AS _RecID2
        , ts._SourceID                                                            AS _SourceID
      FROM salesinvoiceline_factstage                             ts
      LEFT JOIN silver.cma_Date                      dd1
        ON dd1.Date               = ts.InvoiceDate
      LEFT JOIN silver.cma_Date                      dd2
        ON dd2.Date               = ts.CreatedDate
      LEFT JOIN silver.cma_Date                      dd3
        ON dd3.Date               = ts.DueDate
      LEFT JOIN silver.cma_Date                      dd4
        ON dd4.Date               = ts.ShipDate
    INNER JOIN silver.cma_LegalEntity               le
        ON le.LegalEntityID       = ts.LegalEntityID
      LEFT JOIN silver.cma_Customer                  dc
        ON dc.LegalEntityID       = ts.LegalEntityID
      AND dc.CustomerAccount     = ts.OrderAccount
      LEFT JOIN silver.cma_Customer                  dc2
        ON dc2.LegalEntityID      = ts.LegalEntityID
      AND dc2.CustomerAccount    = ts.InvoiceAccount
    INNER JOIN silver.cma_SalesInvoiceLine          dcil
        ON dcil._RecID1           = ts._RecID1
      AND dcil._RecID2           = ts._RecID2
      AND dcil._SourceID         = 1
    INNER JOIN silver.cma_SalesInvoice              si
        ON si._RecID              = ts._RecID1
      AND si._SourceID           = 1
      LEFT JOIN silver.cma_InventorySite             dp
        ON dp.LegalEntityID       = ts.LegalEntityID
      AND dp.InventorySiteID     = ts.SiteID
      LEFT JOIN silver.cma_Warehouse                 dw
        ON dw.LegalEntityID       = ts.LegalEntityID
      AND dw.WarehouseID         = ts.WarehouseID
      LEFT JOIN silver.cma_SalesOrderLine            dsil
        ON dsil._RecID            = ts.RecID_SL
      AND dsil._SourceID         = 1
      LEFT JOIN silver.cma_Product                   dr
        ON dr.LegalEntityID       = ts.LegalEntityID
      AND dr.ItemID              = ts.ItemID
      AND dr.ProductWidth        = ts.ProductWidth
      AND dr.ProductLength       = ts.ProductLength
      AND dr.ProductColor        = ts.ProductColor
      AND dr.ProductConfig       = ts.ProductConfig
      LEFT JOIN silver.cma_SalesPerson               dsp
        ON dsp._RecID             = ts.WorkerSalesResponsible
      AND dsp._SourceID          = 1
      LEFT JOIN silver.cma_Address                   da
        ON da._RecID              = ts.DeliveryPostalAddress
      AND da._SourceID           = 1
      LEFT JOIN silver.cma_Financial                 dca1
        ON dca1._RecID            = ts.DefaultDimension
      AND dca1._SourceID         = 1
      LEFT JOIN silver.cma_Voucher                   vou
        ON vou.LegalEntityID      = ts.LegalEntityID
      AND vou.VoucherID          = ts.VoucherID
      LEFT JOIN silver.cma_Lot                       it
        ON it._recid              = ts.RecID_ITO
      AND it._sourceid           = 1
      LEFT JOIN silver.cma_Currency                  cc
        ON cc.CurrencyID          = ts.CurrencyID
      LEFT JOIN silver.cma_DeliveryMode dm
        ON dm.LegalEntityID       = ts.LegalEntityID
      AND dm.DeliveryModeID      = ts.DeliveryModeID
      LEFT JOIN silver.cma_DeliveryTerm dt
        ON dt.LegalEntityID       = ts.LegalEntityID
      AND dt.DeliveryTermID      = ts.DeliveryTermID
      LEFT JOIN silver.cma_PaymentTerm  pyt
        ON pyt.LegalEntityID      = ts.LegalEntityID
      AND pyt.PaymentTermID      = ts.PaymentTermID
      LEFT JOIN silver.cma_UOM                       pu
        ON pu.UOM                 = ts.PricingUnit
      LEFT JOIN silver.cma_UOM                       su
        ON su.UOM                 = ts.SalesUOM
      LEFT JOIN silver.cma_TaxGroup                  tg
        ON tg.LegalEntityID       = ts.LegalEntityID
      AND tg.TaxGroupID          = ts.TaxGroupID
      LEFT JOIN silver.cma_CashDiscount              cd
        ON cd.LegalEntityID       = ts.LegalEntityID
      AND cd.CashDiscountID      = ts.CashDiscountID
      LEFT JOIN silver.cma_SalesType                 sit
        ON sit.SalesTypeID        = ts.SalesTypeID
      LEFT JOIN silver.cma_ReturnStatus              drs
        ON drs.ReturnStatusID     = ts.ReturnStatus
      LEFT JOIN silver.cma_ReturnReason              drr
        ON drr.LegalEntityID      = ts.LegalEntityID
      AND drr.ReturnReasonID     = ts.ReturnReasonID
      LEFT JOIN salesinvoiceline_factcharge                       ca
        ON ca.SalesInvoiceLineKey = dcil.SalesInvoiceLineKey
      LEFT JOIN silver.cma_Employee                  de2
        ON de2._RecID             = ts.SalesTaker
      LEFT JOIN silver.cma_SalesCategory             dsc
        ON dsc._RecID             = ts.RecID_SC
      AND dsc._SourceID          = 1
      LEFT JOIN silver.cma_InvoiceType               ivt
        ON ivt.InvoiceTypeID      = ts.ReferenceTypeID
      LEFT JOIN silver.cma_PaymentMode  pam
        ON pam.LegalEntityID      = ts.LegalEntityID
      AND pam.PaymentModeID      = ts.PaymentModeID;
),
salesinvoiceline_factline AS (
    SELECT tm.SalesInvoiceLineKey                        AS SalesInvoiceLineKey
        , tm.LegalEntityKey                              AS LegalEntityKey
        , tm.CashDiscountKey                             AS CashDiscountKey
        , tm.CustomerKey                                 AS CustomerKey
        , tm.InvoiceTypeKey                              AS InvoiceTypeKey
        , tm.CurrencyKey                                 AS CurrencyKey
        , tm.DeliveryTermKey                             AS DeliveryTermKey
        , tm.DeliveryModeKey                             AS DeliveryModeKey
        , tm.FinancialKey                                AS FinancialKey
        , tm.SalesInvoiceKey                             AS SalesInvoiceKey
        , tm.PaymentModeKey                              AS PaymentModeKey
        , tm.PaymentTermKey                              AS PaymentTermKey
        , tm.TaxGroupKey                                 AS TaxGroupKey
        , tm.SalesUOMKey                                 AS SalesUOMKey
        , tm.SalesCategoryKey                            AS SalesCategoryKey
        , tm.PricingUOMKey                               AS PricingUOMKey
        , tm.VoucherKey                                  AS VoucherKey
        , tm.DeliveryAddressKey                          AS DeliveryAddressKey
        , tm.InvoiceCustomerKey                          AS InvoiceCustomerKey
        , tm.InvoiceDateKey                              AS InvoiceDateKey
        , tm.CreatedDateKey                              AS CreatedDateKey
        , tm.DueDateKey                                  AS DueDateKey
        , tm.ReturnReasonkey                             AS ReturnReasonkey
        , tm.ReturnStatusKey                             AS ReturnStatusKey
        , tm.ShipDateKey                                 AS ShipDateKey
        , tm.InventorySiteKey                            AS InventorySiteKey
        , tm.LotKey                                      AS LotKey
        , tm.ProductKey                                  AS ProductKey
        , tm.SalesTakerKey                               AS SalesTakerKey
        , tm.SalesTypeKey                                AS SalesTypeKey
        , tm.SalesOrderLineKey                           AS SalesOrderLineKey
        , tm.SalesPersonKey                              AS SalesPersonKey
        , tm.WarehouseKey                                AS WarehouseKey
        , tm.LegalEntityID                               AS LegalEntityID
        , tm.ProductID                                   AS ProductID
        , tm.InventoryUnit                               AS InventoryUnit
        , tm.SalesUOM                                    AS SalesUOM
        , tm.BaseAmount                                  AS BaseAmount
        , tm.BaseAmount_TransCur                         AS BaseAmount_TransCur
        , tm.IncludedCharge                              AS IncludedCharge
        , tm.IncludedCharge_TransCur                     AS IncludedCharge_TransCur
        , tm.AdditionalCharge                            AS AdditionalCharge
        , tm.AdditionalCharge_TransCur                   AS AdditionalCharge_TransCur
        , tm.NonBillableCharge                           AS NonBillableCharge
        , tm.NonBillableCharge_TransCur                  AS NonBillableCharge_TransCur
        , tm.CustomerCharge                              AS CustomerCharge
        , tm.CustomerCharge_TransCur                     AS CustomerCharge_TransCur
        , tm.CostAmount                                  AS CostAmount
        , tm.CostAmount_TransCur                         AS CostAmount_TransCur
        , tm.DiscountAmount                              AS DiscountAmount
        , tm.DiscountAmount_TransCur                     AS DiscountAmount_TransCur
        , tm.NetAmount - tm.CostAmount                   AS GrossProfit
        , tm.NetAmount_TransCur - tm.CostAmount_TransCur AS GrossProfit_TransCur
        , ISNULL (tm.BaseAmount, 0) + ISNULL (tm.CustomerCharge, 0)
          + ISNULL (CASE WHEN tm.DiscountAmount < 0 THEN tm.DiscountAmount ELSE tm.DiscountAmount * -1 END, 0)
          + ISNULL (tm.TaxAmount, 0)                     AS InvoiceTotalAmount
        , ISNULL (tm.BaseAmount_TransCur, 0) + ISNULL (tm.CustomerCharge_TransCur, 0)
          + ISNULL (
                CASE WHEN tm.DiscountAmount_TransCur < 0 THEN tm.DiscountAmount_TransCur ELSE
                                                                                          tm.DiscountAmount_TransCur
                                                                                          * -1 END
              , 0) + ISNULL (tm.TaxAmount_TransCur, 0)   AS InvoiceTotalAmount_TransCur
        , tm.InvoiceQuantity_SalesUOM                    AS InvoiceQuantity_SalesUOM
        , tm.InvoiceQuantity                             AS InvoiceQuantity
        , tm.NetAmount                                   AS NetAmount
        , tm.NetAmount_TransCur                          AS NetAmount_TransCur
        , tm.InvoiceSalesAmount                          AS InvoiceSalesAmount
        , tm.InvoiceSalesAmount_TransCur                 AS InvoiceSalesAmount_TransCur
        , tm.TaxAmount                                   AS TaxAmount
        , tm.TaxAmount_TransCur                          AS TaxAmount_TransCur
        , tm.BaseUnitPrice                               AS BaseUnitPrice
        , tm.BaseUnitPrice_TransCur                      AS BaseUnitPrice_TransCur
        , tm.TotalUnitPrice                              AS TotalUnitPrice
        , tm.TotalUnitPrice_TransCur                     AS TotalUnitPrice_TransCur
        , tm.PriceUnit                                   AS PriceUnit
        , tm._SourceDate
        , tm._RecID1                                     AS _RecID1
        , tm._RecID2                                     AS _RecID2
        , tm._SourceID                                   AS _SourceID
      FROM salesinvoiceline_factmain tm;
)
SELECT tl.SalesInvoiceLineKey
      , tl.CashDiscountKey
      , tl.CreatedDateKey
      , tl.CurrencyKey
      , tl.CustomerKey
      , tl.DeliveryAddressKey
      , tl.DeliveryModeKey
      , tl.DeliveryTermKey
      , tl.DueDateKey
      , tl.FinancialKey
      , tl.InvoiceCustomerKey
      , tl.InvoiceDateKey
      , tl.InvoiceTypeKey
      , tl.LegalEntityKey
      , tl.LotKey
      , tl.PaymentModeKey
      , tl.PaymentTermKey
      , tl.PricingUOMKey
      , tl.ProductKey
      , tl.ReturnReasonkey
      , tl.ReturnStatusKey
      , tl.SalesCategoryKey
      , tl.SalesInvoiceKey
      , tl.SalesOrderLineKey
      , tl.SalesPersonKey
      , tl.SalesTakerKey
      , tl.SalesTypeKey
      , tl.SalesUOMKey
      , tl.ShipDateKey
      , tl.InventorySiteKey
      , tl.TaxGroupKey
      , tl.VoucherKey
      , tl.WarehouseKey
      , tl.AdditionalCharge
      , tl.AdditionalCharge_TransCur
      , tl.BaseAmount
      , tl.BaseAmount_TransCur
      , tl.BaseUnitPrice
      , tl.BaseUnitPrice_TransCur
      , tl.CustomerCharge
      , tl.CustomerCharge_TransCur
      , tl.CostAmount
      , tl.CostAmount_TransCur
      , CASE WHEN tl.DiscountAmount < 0 THEN tl.DiscountAmount * -1 ELSE tl.DiscountAmount END                       AS DiscountAmount
      , CASE WHEN tl.DiscountAmount_TransCur < 0 THEN tl.DiscountAmount_TransCur * -1 ELSE
                                                                                      tl.DiscountAmount_TransCur END AS DiscountAmount_TransCur
      , tl.GrossProfit                                                                                               AS GrossProfit
      , tl.GrossProfit_TransCur                                                                                      AS GrossProfit_TransCur
      , tl.IncludedCharge
      , tl.IncludedCharge_TransCur
      , tl.InvoiceTotalAmount
      , tl.InvoiceTotalAmount_TransCur
      , tl.InvoiceQuantity
      , tl.InvoiceQuantity_SalesUOM
      , tl.InvoiceQuantity_SalesUOM * ISNULL (vuc.factor, 0)                                                         AS InvoiceQuantity_FT
      , tl.InvoiceQuantity_SalesUOM * ISNULL (vuc2.factor, 0)                                                        AS InvoiceQuantity_LB
      , ROUND (tl.InvoiceQuantity_SalesUOM * ISNULL (vuc3.factor, 0), 0)                                             AS InvoiceQuantity_PC
      , tl.InvoiceQuantity_SalesUOM * ISNULL (vuc4.factor, 0)                                                        AS InvoiceQuantity_SQIN
      , tl.NetAmount
      , tl.NetAmount_TransCur
      , tl.NonBillableCharge
      , tl.NonBillableCharge_TransCur
      , tl.PriceUnit
      , tl.TaxAmount
      , tl.TaxAmount_TransCur
      , tl.InvoiceSalesAmount
      , tl.InvoiceSalesAmount_TransCur
      , tl.TotalUnitPrice
      , tl.TotalUnitPrice_TransCur
      , tl._SourceDate
      , tl._RecID1                                                                                                   AS _RecID1
      , tl._RecID2                                                                                                   AS _RecID2
      , tl._SourceID
      , CURRENT_TIMESTAMP AS _CreatedDate
      , CURRENT_TIMESTAMP AS _ModifiedDate
    FROM salesinvoiceline_factline                    tl
    LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc
      ON vuc.legalentitykey  = tl.LegalEntityKey
    AND vuc.productkey       = tl.ProductKey
    AND vuc.fromuomkey       = tl.SalesUOMKey
--  AND vuc.touom            = 'FT'
    LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc2
      ON vuc2.legalentitykey = tl.LegalEntityKey
    AND vuc2.productkey      = tl.ProductKey
    AND vuc2.fromuomkey      = tl.SalesUOMKey
--  AND vuc2.touom           = 'LB'
    LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc3
      ON vuc3.legalentitykey = tl.LegalEntityKey
    AND vuc3.productkey      = tl.ProductKey
    AND vuc3.fromuomkey      = tl.SalesUOMKey
--  AND vuc3.touom          = 'PC'
    LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc4
      ON vuc4.legalentitykey = tl.LegalEntityKey
    AND vuc4.productkey      = tl.ProductKey
    AND vuc4.fromuomkey      = tl.SalesUOMKey
--  AND vuc4.touom           = 'SQIN';
