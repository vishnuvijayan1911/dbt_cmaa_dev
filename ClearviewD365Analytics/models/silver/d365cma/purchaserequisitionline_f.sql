{{ config(materialized='table', tags=['silver'], alias='purchaserequisitionline_fact') }}

-- Source file: cma/cma/layers/_base/_silver/purchaserequisitionline_f/purchaserequisitionline_f.py
-- Root method: PurchaserequisitionlineFact.purchaserequisitionline_factdetail [PurchaseRequisitionLine_FactDetail]
-- Inlined methods: PurchaserequisitionlineFact.purchaserequisitionline_factrequisitionpol [PurchaseRequisitionLine_FactRequisitionPOL], PurchaserequisitionlineFact.purchaserequisitionline_factstage [PurchaseRequisitionLine_FactStage], PurchaserequisitionlineFact.purchaserequisitionline_factdetail1 [PurchaseRequisitionLine_FactDetail1]
-- external_table_name: PurchaseRequisitionLine_FactDetail
-- schema_name: temp

WITH
purchaserequisitionline_factrequisitionpol AS (
    SELECT prl.recid     AS RecID_PRL
             , MAX(pl.recid) AS RecID_PL

          FROM {{ ref('purchreqline') }}    prl
          JOIN {{ ref('purchline') }}       pl
            ON pl.purchreqlinerefid = prl.linerefid
         INNER JOIN {{ ref('purchtable') }} pt
            ON pt.dataareaid       = pl.dataareaid
           AND pt.purchid           = pl.purchid
         GROUP BY prl.recid;
),
purchaserequisitionline_factstage AS (
    SELECT prl.transdate
             , prl.inventdimiddataarea                                                        AS LegalEntityID
             , CAST(CAST(prl.createddatetime AS Datetime) AT TIME ZONE 'UTC' AT TIME ZONE le.TimeZone AS DATE) AS CreatedDate
             , prl.currencycode                                                               AS CurrencyID
             , id.inventsizeid                                                                AS InventSizeID
             , id.inventcolorid                                                               AS InventColorID
             , id.inventstyleid                                                               AS InventStyleID
             , id.configid                                                                    AS InventConfigID
             , prl.vendaccount                                                                AS VendorAccount
             , pt.itembuyergroupid                                                            AS BuyerGroupID
             , prl.requisitioner                                                              AS Employee
             , prl.procurementcategory                                                        AS ProcurementCategory
             , prl.requireddate                                                               AS RequiredDate
             , id.inventsiteid                                                                AS SiteID
             , prt.requisitionstatus                                                          AS RequisitionStatusID
             , prl.requisitionstatus                                                          AS RequisitionLineStatusID
             , prl.linetype                                                                   AS RequisitionTypeID
             , uom.symbol                                                                     AS PurchaseUnit
             , prl.lineamount                                                                 AS NetAmount_TransCur
             , prl.purchmarkup                                                                AS ChargeAmount_TransCur
             , prl.linedisc                                                                   AS DiscountAmount_TransCur
             , prl.linepercent                                                                AS DiscountPercent
             , prl.priceunit                                                                  AS PriceUnit
             , pt.documentstate                                                               AS DocumentStateID
             , prl.purchqty                                                                   AS Quantity_PurchUOM
             , prl.purchprice                                                                 AS PurchasePrice_TransCur
             , prl.purchqty                                                                   AS Quantity_InventUnit
             , prl.itemid                                                                     AS ItemID
             , tpl.RecID_PL                                                                   AS RecID_PL
             , prl.recid                                                                      AS RecID_PRL

          FROM {{ ref('purchreqline') }}       prl
         INNER JOIN {{ ref('purchreqtable') }} prt
            ON prt.recid       = prl.purchreqtable
         INNER JOIN {{ ref('inventdim') }}     id
            ON id.dataareaid   = prl.inventdimiddataarea
           AND id.inventdimid   = prl.inventdimid
         INNER JOIN silver.cma_LegalEntity   le
            ON le.LegalEntityID = prl.inventdimiddataarea
          LEFT JOIN {{ ref('unitofmeasure') }} uom
            ON uom.recid       = prl.purchunitofmeasure
          LEFT JOIN purchaserequisitionline_factrequisitionpol  tpl
            ON tpl.RecID_PRL    = prl.recid
          LEFT JOIN {{ ref('purchline') }}     pl
            ON pl.recid        = tpl.RecID_PL
          LEFT JOIN {{ ref('purchtable') }}    pt
            ON pt.dataareaid   = pl.dataareaid
           AND pt.purchid       = pl.purchid;
),
purchaserequisitionline_factdetail1 AS (
    SELECT dprl.PurchaseRequisitionLineKey                         AS PurchaseRequisitionLineKey
             , ds.DocumentStateKey                                     AS DocumentStateKey
             , cur.CurrencyKey                                         AS CurrencyKey
             , le.LegalEntityKey                                       AS LegalEntityKey
             , dd1.DateKey                                             AS CreatedDateKey
             , dis.InventorySiteKey                                    AS InventorySiteKey
             , dpol.PurchaseOrderLineKey                               AS PurchaseOrderLineKey
             , dbg.BuyerGroupKey                                       AS BuyerGroupKey
             , dpc.ProcurementCategoryKey                              AS ProcurementCategoryKey
             , dp.ProductKey                                           AS ProductKey
             , pu.UOMKey                                               AS PurchaseUOMKey
             , drs.RequisitionStatusKey                                AS RequisitionLineStatusKey
             , drs1.RequisitionStatusKey                               AS RequisitionStatusKey
             , drt.RequisitionTypeKey                                  AS RequisitionTypeKey
             , dd.DateKey                                              AS RequiredDateKey
             , dv.VendorKey                                            AS VendorKey
             , ts.NetAmount_TransCur * ISNULL(ex.ExchangeRate, 1)      AS NetAmount
             , ts.NetAmount_TransCur                                   AS NetAmount_TransCur
             , ts.ChargeAmount_TransCur * ISNULL(ex.ExchangeRate, 1)   AS ChargeAmount
             , ts.ChargeAmount_TransCur                                AS ChargeAmount_TransCur
             , ts.DiscountAmount_TransCur * ISNULL(ex.ExchangeRate, 1) AS DiscountAmount
             , ts.DiscountAmount_TransCur                              AS DiscountAmount_TransCur
             , ts.DiscountPercent                                      AS DiscountPercent
             , ts.Quantity_PurchUOM
             , ts.PriceUnit                                            AS PriceUnit
             , ts.PurchasePrice_TransCur * ISNULL(ex.ExchangeRate, 1)  AS BasePrice
             , ts.PurchasePrice_TransCur                               AS BasePrice_TransCur
             , CAST(1 AS INT)                                          AS RequisitionLineCount
             , le.AccountingCurrencyID
             , le.TransExchangeRateType
             , ts.RecID_PRL                                            AS _RecID
             , 1                                                       AS _SourceID

          FROM purchaserequisitionline_factstage                          ts
         INNER JOIN silver.cma_PurchaseRequisitionLine dprl
            ON dprl._RecID              = ts.RecID_PRL
           AND dprl._SourceID           = 1
         INNER JOIN silver.cma_Date                    dd1
            ON dd1.Date                 = ts.CreatedDate
         INNER JOIN silver.cma_LegalEntity             le
            ON le.LegalEntityID         = dprl.LegalEntityID
          LEFT JOIN silver.cma_InventorySite           dis
            ON dis.LegalEntityID        = ts.LegalEntityID
           AND dis.InventorySiteID      = ts.SiteID
          LEFT JOIN silver.cma_PurchaseOrderLine       dpol
            ON dpol._RecID              = ts.RecID_PL
           AND dpol._SourceID           = 1
          LEFT JOIN silver.cma_BuyerGroup              dbg
            ON dbg.LegalEntityID        = ts.LegalEntityID
           AND dbg.BuyerGroupID         = ts.BuyerGroupID
          LEFT JOIN silver.cma_Product                 dp
            ON dp.LegalEntityID         = ts.LegalEntityID
           AND dp.ItemID                = ts.ItemID
           AND dp.ProductWidth          = ts.InventSizeID
           AND dp.ProductLength         = ts.InventColorID
           AND dp.ProductColor          = ts.InventStyleID
           AND dp.ProductConfig         = ts.InventConfigID
         INNER JOIN silver.cma_Date                    dd
            ON dd.Date                  = ts.RequiredDate
          LEFT JOIN silver.cma_Vendor                  dv
            ON dv.LegalEntityID         = ts.LegalEntityID
           AND dv.VendorAccount         = ts.VendorAccount
          LEFT JOIN silver.cma_RequisitionStatus       drs
            ON drs.RequisitionStatusID  = ts.RequisitionLineStatusID
          LEFT JOIN silver.cma_RequisitionStatus       drs1
            ON drs1.RequisitionStatusID = ts.RequisitionStatusID
          LEFT JOIN silver.cma_RequisitionType         drt
            ON drt.RequisitionTypeID    = ts.RequisitionTypeID
          LEFT JOIN silver.cma_UOM                     pu
            ON pu.UOM                   = ts.PurchaseUnit
          LEFT JOIN silver.cma_ProcurementCategory     dpc
            ON dpc._RecID               = ts.ProcurementCategory
           AND dpc._SourceID            = 1
          LEFT JOIN silver.cma_Currency                cur
            ON cur.CurrencyID           = ts.CurrencyID
          LEFT JOIN silver.cma_ExchangeRate_Fact       ex
            ON ex.ExchangeDateKey       = dd.DateKey
           AND ex.FromCurrencyID        = ts.CurrencyID
           AND ex.ToCurrencyID          = le.AccountingCurrencyID
           AND ex.ExchangeRateType      = le.TransExchangeRateType
          LEFT JOIN silver.cma_DocumentState           ds
            ON ds.DocumentStateID       = ts.DocumentStateID;
)
SELECT 
           CURRENT_TIMESTAMP                                                                      AS _CreatedDate
         , CURRENT_TIMESTAMP                                                                      AS _ModifiedDate 
         , td.PurchaseRequisitionLineKey
         , td.CurrencyKey
         , td.LegalEntityKey
         , td.CreatedDateKey
         , td.InventorySiteKey
         , td.PurchaseOrderLineKey
         , td.BuyerGroupKey
         , td.ProcurementCategoryKey
         , td.DocumentStateKey
         , td.ProductKey
         , td.PurchaseUOMKey
         , td.RequisitionLineStatusKey
         , td.RequisitionStatusKey
         , td.RequisitionTypeKey
         , td.RequiredDateKey
         , td.VendorKey
         , td.NetAmount
         , td.NetAmount_TransCur
         , td.ChargeAmount
         , td.ChargeAmount_TransCur
         , td.DiscountAmount
         , td.DiscountAmount_TransCur
         , td.DiscountPercent
         , td.Quantity_PurchUOM									   AS RequisitionQuantity
         , td.Quantity_PurchUOM * ISNULL(vuc.factor, 0)            AS RequisitionQuantity_FT

         , td.Quantity_PurchUOM * ISNULL(vuc2.factor, 0)           AS RequisitionQuantity_LB
         , ROUND(td.Quantity_PurchUOM * ISNULL(vuc3.factor, 0), 0) AS RequisitionQuantity_PC
         , td.Quantity_PurchUOM * ISNULL(vuc4.factor, 0)           AS RequisitionQuantity_SQIN

         , td.PriceUnit
         , td.BasePrice
         , td.BasePrice_TransCur
         , td.RequisitionLineCount
         , td._RecID
         , td._SourceID

      FROM purchaserequisitionline_factdetail1                 td
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
