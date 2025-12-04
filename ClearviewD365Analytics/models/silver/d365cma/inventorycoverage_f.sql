{{ config(materialized='table', tags=['silver'], alias='inventorycoverage_fact') }}

-- Source file: cma/cma/layers/_base/_silver/inventorycoverage_f/inventorycoverage_f.py
-- Root method: InventorycoverageFact.inventorycoverage_factdetail [InventoryCoverage_FactDetail]
-- Inlined methods: InventorycoverageFact.inventorycoverage_factonhand [InventoryCoverage_FactOnHand], InventorycoverageFact.inventorycoverage_factline [InventoryCoverage_FactLine], InventorycoverageFact.inventorycoverage_factdetailmain [InventoryCoverage_FactDetailMain]
-- external_table_name: InventoryCoverage_FactDetail
-- schema_name: temp

WITH
inventorycoverage_factonhand AS (
    SELECT rit.recid              AS RecID_RIT
             , SUM(isu.physicalinvent) AS PhysicalInventory
             , SUM(isu.availphysical)  AS NettableQuantity

          FROM {{ ref('reqitemtable') }}   rit
         INNER JOIN {{ ref('inventsum') }} isu
            ON isu.dataareaid = rit.dataareaid
           AND isu.itemid      = rit.itemid
           AND isu.inventdimid = rit.covinventdimid
         GROUP BY rit.recid;
),
inventorycoverage_factline AS (
    SELECT rit.dataareaid             AS LegalEntityID
             , rit.itemid                  AS ItemID
             , rit.reqpotype               AS InventoryMakeOrBuyID
             , rit.reqgroupid              AS InventoryCoverageGroupID
             , iil.countgroupid            AS InventoryCountingGroupID
             , id.inventsiteid             AS InventSiteID
             , id.inventlocationid         AS InventLocationID
             , rit.inventlocationidreqmain AS InventLocationIDReqMain
             , id.inventsizeid             AS InventSizeID
             , id.inventcolorid            AS InventColorID
             , id.inventstyleid            AS InventStyleID
             , id.configid                 AS ProductConfig
             , rit.authorizationtimefence  AS AuthorizationTimeFence
             , rit.mininventonhand         AS MinimumQuantity
             , rit.maxinventonhand         AS MaximumQuantity
             , itmp.price                  AS PurchasePrice
             , itmi.price                  AS ReleasedProductPrice
             , rit.leadtimepurchase        AS PurchaseLeadDays
             , rit.recid                  AS _RecID

          FROM {{ ref('reqitemtable') }}            rit
         INNER JOIN {{ ref('inventdim') }}          id
            ON id.dataareaid   = rit.dataareaid
           AND id.inventdimid   = rit.covinventdimid
         INNER JOIN {{ ref('inventtablemodule') }}  itmp
            ON itmp.dataareaid = rit.dataareaid
           AND itmp.itemid      = rit.itemid
           AND itmp.moduletype  = 1 
         INNER JOIN {{ ref('inventtablemodule') }}  itmi
            ON itmi.dataareaid = rit.dataareaid
           AND itmi.itemid      = rit.itemid
           AND itmi.moduletype  = 0 
          LEFT JOIN {{ ref('inventitemlocation') }} iil
            ON iil.dataareaid  = rit.dataareaid
           AND iil.itemid       = rit.itemid
           AND iil.inventdimid  = rit.covinventdimid
          LEFT JOIN {{ ref('inventcountgroup') }}   icg
            ON icg.dataareaid  = iil.dataareaid
           AND icg.countgroupid = iil.countgroupid;
),
inventorycoverage_factdetailmain AS (
    SELECT le.LegalEntityKey             AS LegalEntityKey
             , ds.InventorySiteKey           AS InventorySiteKey
             , imob.InventoryMakeOrBuyKey    AS InventoryMakeOrBuyKey
             , mmc.InventoryCountingGroupKey AS InventoryCountingGroupKey
             , ic.InventoryCoverageGroupKey  AS InventoryCoverageGroupKey
             , ISNULL(dp.ProductKey, -1)     AS ProductKey
             , dw2.WarehouseKey              AS TransferWarehouseKey
             , dw.WarehouseKey               AS WarehouseKey
             , du.UOMKey                     AS InventoryUnitKey
             , li.AuthorizationTimeFence     AS AuthorizationTimeFence
             , li.MinimumQuantity            AS MinimumQuantity
             , li.MaximumQuantity            AS MaximumQuantity
             , toh.NettableQuantity          AS NettableQuantity
             , toh.PhysicalInventory         AS PhysicalInventory
             , li.PurchasePrice              AS PurchasePrice
             , li.ReleasedProductPrice       AS ReleasedProductPrice
             , li.PurchaseLeadDays           AS PurchaseLeadDays
             , li._RecID                     AS _RecID
             , 1                             AS _SourceID

          FROM inventorycoverage_factline                           li
         INNER JOIN {{ ref('legalentity_d') }}            le
            ON le.LegalEntityID             = li.LegalEntityID
          LEFT JOIN {{ ref('inventorysite_d') }}          ds
            ON ds.LegalEntityID             = li.LegalEntityID
           AND ds.InventorySiteID           = li.InventSiteID
          LEFT JOIN {{ ref('warehouse_d') }}              dw
            ON dw.LegalEntityID             = li.LegalEntityID
           AND dw.WarehouseID               = li.InventLocationID
          LEFT JOIN {{ ref('warehouse_d') }}              dw2
            ON dw2.LegalEntityID            = li.LegalEntityID
           AND dw2.WarehouseID              = li.InventLocationIDReqMain
          LEFT JOIN {{ ref('product_d') }}                dp
            ON dp.LegalEntityID             = li.LegalEntityID
           AND dp.ItemID                    = li.ItemID
           AND dp.ProductWidth              = li.InventSizeID
           AND dp.ProductLength             = li.InventColorID
           AND dp.ProductColor              = li.InventStyleID
           AND dp.ProductConfig             = li.ProductConfig
         INNER JOIN {{ ref('uom_d') }}                    du
            ON du.UOM                       = dp.InventoryUOM
          LEFT JOIN inventorycoverage_factonhand                    toh
            ON toh.RecID_RIT                = li._RecID
          LEFT JOIN {{ ref('inventorymakeorbuy_d') }}     imob
            ON imob.InventoryMakeOrBuyID    = li.InventoryMakeOrBuyID
          LEFT JOIN {{ ref('inventorycountinggroup_d') }} mmc
            ON mmc.LegalEntityID            = li.LegalEntityID
           AND mmc.InventoryCountingGroupID = li.InventoryCountingGroupID
          LEFT JOIN {{ ref('inventorycoveragegroup_d') }} ic
            ON ic.LegalEntityID             = li.LegalEntityID
           AND ic.InventoryCoverageGroupID  = li.InventoryCoverageGroupID;
)
SELECT  {{ dbt_utils.generate_surrogate_key(['td._RecID', 'td._SourceID']) }} AS InventoryCoverageKey
         , td.LegalEntityKey
         , td.ProductKey
         , td.InventorySiteKey
         , td.InventoryMakeOrBuyKey
         , td.InventoryCountingGroupKey
         , td.InventoryCoverageGroupKey
         , td.TransferWarehouseKey
         , td.WarehouseKey
         , td.MinimumQuantity
         , td.MinimumQuantity * ISNULL(vuc.factor, 0)              AS MinimumQuantity_FT

         , td.MinimumQuantity * ISNULL(vuc2.factor, 0)             AS MinimumQuantity_LB
         , ROUND(td.MinimumQuantity * ISNULL(vuc3.factor, 0), 0)   AS MinimumQuantity_PC
         , td.MinimumQuantity * ISNULL(vuc4.factor, 0)             AS MinimumQuantity_SQIN

         , td.MaximumQuantity
         , td.MaximumQuantity * ISNULL(vuc.factor, 0)              AS MaximumQuantity_FT

         , td.MaximumQuantity * ISNULL(vuc2.factor, 0)             AS MaximumQuantity_LB
         , ROUND(td.MaximumQuantity * ISNULL(vuc3.factor, 0), 0)   AS MaximumQuantity_PC
         , td.MaximumQuantity * ISNULL(vuc4.factor, 0)             AS MaximumQuantity_SQIN

         , td.NettableQuantity
         , td.NettableQuantity * ISNULL(vuc.factor, 0)             AS NettableQuantity_FT

         , td.NettableQuantity * ISNULL(vuc2.factor, 0)            AS NettableQuantity_LB
         , ROUND(td.NettableQuantity * ISNULL(vuc3.factor, 0), 0)  AS NettableQuantity_PC
         , td.NettableQuantity * ISNULL(vuc4.factor, 0)            AS NettableQuantity_SQIN

         , td.AuthorizationTimeFence
         , td.PhysicalInventory
         , td.PhysicalInventory * ISNULL(vuc.factor, 0)            AS PhysicalInventory_FT

         , td.PhysicalInventory * ISNULL(vuc2.factor, 0)           AS PhysicalInventory_LB
         , ROUND(td.PhysicalInventory * ISNULL(vuc3.factor, 0), 0) AS PhysicalInventory_PC
         , td.PhysicalInventory * ISNULL(vuc4.factor, 0)           AS PhysicalInventory_SQIN

         , td.PurchaseLeadDays
         , td.PurchasePrice
         , td.ReleasedProductPrice
         , td._SourceID
         , td._RecID
         ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))  AS  _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
      FROM inventorycoverage_factdetailmain              td
      LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc
        ON vuc.productkey      = td.ProductKey
       AND vuc.fromuomkey      = td.InventoryUnitKey
    -- AND vuc.touom           = 'FT'
       AND vuc.legalentitykey  = td.LegalEntityKey
      LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc2
        ON vuc2.productkey     = td.ProductKey
       AND vuc2.fromuomkey     = td.InventoryUnitKey
    -- AND vuc2.touom          = 'LB'
       AND vuc2.legalentitykey = td.LegalEntityKey
      LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc3
        ON vuc3.productkey     = td.ProductKey
       AND vuc3.fromuomkey     = td.InventoryUnitKey
    -- AND vuc3.touom          = 'PC'
       AND vuc3.legalentitykey = td.LegalEntityKey
      LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc4
        ON vuc4.productkey     = td.ProductKey
       AND vuc4.fromuomkey     = td.InventoryUnitKey
    -- AND vuc4.touom          = 'SQIN'
       AND vuc4.legalentitykey = td.LegalEntityKey
