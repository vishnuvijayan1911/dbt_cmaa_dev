{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/productioncoproduct_fact/productioncoproduct_fact.py
-- Root method: ProductioncoproductFact.productioncoproduct_factdetail [ProductionCoProduct_FactDetail]
-- Inlined methods: ProductioncoproductFact.productioncoproduct_factstage [ProductionCoProduct_FactStage], ProductioncoproductFact.productioncoproduct_factdetail1 [ProductionCoProduct_FactDetail1]
-- external_table_name: ProductionCoProduct_FactDetail
-- schema_name: temp

WITH
productioncoproduct_factstage AS (
    SELECT pr.itemid            AS ItemID
             , pr.dataareaid       AS LegalEntityID
             , pr.defaultdimension  AS DefaultDimension
             , pr.prodid            AS ProductionID
             , id.inventcolorid     AS ProductLength
             , id.inventstyleid     AS ProductColor
             , id.inventsizeid      AS ProductWidth
             , id.configid          AS ProductConfig
             , pr.cobyqty           AS CoByQuantity
             , pr.costallocationpct AS CostAllocationPercent
             , pr.cobyqtyserie      AS PerSeriesQuantity
             , pr.recid              AS _RecID
             , 1                    AS _SourceID
          FROM {{ ref('pmfprodcoby') }}    pr
          LEFT JOIN {{ ref('inventdim') }} id
            ON id.dataareaid = pr.dataareaid
           AND id.inventdimid = pr.inventdimid;
),
productioncoproduct_factdetail1 AS (
    SELECT pp.ProductionCoProductKey AS ProductionCoProductKey
             , po.ProductionKey          AS ProductionKey
             , le.LegalEntityKey         AS LegalEntityKey
             , fd.FinancialKey           AS FinancialKey
             , du.UOMKey                 AS InventoryUOMKey
             , ISNULL(dp.ProductKey, -1) AS ProductKey
             , ts.CoByQuantity           AS CoByQuantity
             , ts.CostAllocationPercent  AS CostAllocationPercent
             , ts.PerSeriesQuantity      AS PerSeriesQuantity
             , ts._RecID                 AS _RecID
             , ts._SourceID              AS _SourceID
          FROM productioncoproduct_factstage                      ts
         INNER JOIN silver.cma_ProductionCoProduct pp
            ON pp._RecID        = ts._RecID
           AND pp._SourceID     = 1
          LEFT JOIN silver.cma_Product             dp
            ON dp.LegalEntityID = ts.LegalEntityID
           AND dp.ItemID        = ts.ItemID
           AND dp.ProductLength = ts.ProductLength
           AND dp.ProductColor  = ts.ProductColor
           AND dp.ProductWidth  = ts.ProductWidth
           AND dp.ProductConfig = ts.ProductConfig
          LEFT JOIN silver.cma_Production          po
            ON po.LegalEntityID = ts.LegalEntityID
           AND po.ProductionID  = ts.ProductionID
         INNER JOIN silver.cma_LegalEntity         le
            ON le.LegalEntityID = ts.LegalEntityID
          LEFT JOIN silver.cma_Financial           fd
            ON fd._RecID        = ts.DefaultDimension
           AND fd._SourceID     = 1
          LEFT JOIN silver.cma_UOM                 du
            ON du.UOM           = dp.InventoryUOM;
)
SELECT td.ProductionCoProductKey
         , td.ProductionKey
         , td.LegalEntityKey
         , td.FinancialKey
         , td.ProductKey
         , td.CoByQuantity
         , td.CoByQuantity * ISNULL(vuc.factor, 0)                 AS CoByQuantity_FT

         , td.CoByQuantity * ISNULL(vuc2.factor, 0)                AS CoByQuantity_LB
         , ROUND(td.CoByQuantity * ISNULL(vuc3.factor, 0), 0)      AS CoByQuantity_PC
         , td.CoByQuantity * ISNULL(vuc4.factor, 0)                AS CoByQuantity_SQIN

         , td.PerSeriesQuantity
         , td.PerSeriesQuantity * ISNULL(vuc.factor, 0)            AS PerSeriesQuantity_FT

         , td.PerSeriesQuantity * ISNULL(vuc2.factor, 0)           AS PerSeriesQuantity_LB
         , ROUND(td.PerSeriesQuantity * ISNULL(vuc3.factor, 0), 0) AS PerSeriesQuantity_PC
         , td.PerSeriesQuantity * ISNULL(vuc4.factor, 0)           AS PerSeriesQuantity_SQIN

         , td.CostAllocationPercent
         , td._SourceID
         , td._RecID
         ,  CURRENT_TIMESTAMP  AS  _CreatedDate
         , CURRENT_TIMESTAMP AS _ModifiedDate
      FROM  productioncoproduct_factdetail1            td
      LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc
        ON vuc.legalentitykey  = td.LegalEntityKey
       AND vuc.productkey      = td.ProductKey
       AND vuc.fromuomkey      = td.InventoryUOMKey
    -- AND vuc.touom           = 'FT'





      LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc2
        ON vuc2.legalentitykey = td.LegalEntityKey
       AND vuc2.productkey     = td.ProductKey
       AND vuc2.fromuomkey     = td.InventoryUOMKey
    -- AND vuc2.touom          = 'LB'
      LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc3
        ON vuc3.legalentitykey = td.LegalEntityKey
       AND vuc3.productkey     = td.ProductKey
       AND vuc3.fromuomkey     = td.InventoryUOMKey
    -- AND vuc3.touom          = 'PC'
      LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc4
        ON vuc4.legalentitykey = td.LegalEntityKey
       AND vuc4.productkey     = td.ProductKey
       AND vuc4.fromuomkey     = td.InventoryUOMKey
    -- AND vuc4.touom          = 'SQIN'
