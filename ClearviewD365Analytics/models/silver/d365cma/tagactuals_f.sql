{{ config(materialized='table', tags=['silver'], alias='tagactuals_fact') }}

-- Source file: cma/cma/layers/_base/_silver/tagactuals_f/tagactuals_f.py
-- Root method: TagactualsFact.tagactuals_factdetail [TagActuals_FactDetail]
-- Inlined methods: TagactualsFact.tagactuals_factsumunit [TagActuals_FactSumUnit], TagactualsFact.tagactuals_factstage [TagActuals_FactStage], TagactualsFact.tagactuals_factdetailmain [TagActuals_FactDetailMain]
-- external_table_name: TagActuals_FactDetail
-- schema_name: temp

WITH
tagactuals_factsumunit AS (
    SELECT tat.recid
             , itm.cmasecondaryuom AS TagactualsSUMUnit
          FROM {{ ref('tagactualstable') }}     tat
          LEFT JOIN {{ ref('inventtable') }}       it
            ON it.dataareaid  = tat.dataareaid
           AND it.itemid       = tat.itemid
          LEFT JOIN {{ ref('inventtablemodule') }} itm
            ON itm.dataareaid = it.dataareaid
           AND itm.itemid      = it.itemid
         WHERE tat.inventbatchid   <> ''
           AND itm.cmasecondaryuom <> '';
),
tagactuals_factstage AS (
    SELECT tat.qty    AS TagActualsQuantity
             , tat.unitid AS TagActualsUnitID
             , tat.inventbatchid
             , tat.itemid
             , id.inventcolorid
             , id.inventsizeid
             , id.inventstyleid
             , id.configid
             , tat.dataareaid
             , su.TagactualsSUMUnit
             , ipr.cmaweightuom
             , tat.recid  AS _RecID
             , 1          AS _SourceID
          FROM {{ ref('tagactualstable') }}    tat
          LEFT JOIN {{ ref('inventparameters') }} ipr
            ON ipr.dataareaid = tat.dataareaid
          LEFT JOIN {{ ref('inventdim') }}        id
            ON id.dataareaid  = tat.dataareaid
           AND id.inventdimid  = tat.inventdimid
          LEFT JOIN tagactuals_factsumunit             su
            ON su.RECID        = tat.recid;
),
tagactuals_factdetailmain AS (
    SELECT le.LegalEntityKey
             , ISNULL(dp.ProductKey, -1) AS ProductKey
             , it.tagkey
             , uo.UOMKey                 AS UOMKey
             , uo1.UOMKey                AS SumUOMKey
             , uo2.UOMKey                AS MassUOMKey
             , ts.TagActualsQuantity     AS Quantity
             , ts._RecID
             , ts._SourceID

          FROM tagactuals_factstage               ts
         INNER JOIN {{ ref('legalentity_d') }} le
            ON le.LegalEntityID = ts.DATAAREAID
          LEFT JOIN {{ ref('product_d') }}     dp
            ON dp.LegalEntityID = ts.DATAAREAID
           AND dp.ItemID        = ts.ITEMID
           AND dp.ProductLength = ts.INVENTCOLORID
           AND dp.ProductColor  = ts.INVENTSTYLEID
           AND dp.ProductWidth  = ts.INVENTSIZEID
           AND dp.ProductConfig = ts.CONFIGID
          LEFT JOIN {{ ref('tag_d') }}         it
            ON it.legalentityid = ts.DATAAREAID
           AND it.itemid        = ts.ITEMID
           AND it.tagid         = ts.INVENTBATCHID
          LEFT JOIN {{ ref('uom_d') }}         uo
            ON uo.UOM           = ts.TagActualsUnitID
          LEFT JOIN {{ ref('uom_d') }}         uo1
            ON uo1.UOM          = ts.TagactualsSUMUnit
          LEFT JOIN {{ ref('uom_d') }}         uo2
            ON uo2.UOM          = ts.CMAWEIGHTUOM;
)
SELECT 
         , {{ dbt_utils.generate_surrogate_key(['dm._RecID', 'dm._SourceID']) }} AS TagActualsKey
        , dm.LegalEntityKey
         , dm.ProductKey
         , dm.TagKey
         , dm.UOMKey
         , dm.SumUOMKey
         , dm.MassUOMKey
         , dm.Quantity
         , dm.Quantity * ISNULL(vuc1.factor, 0) AS SUMPerPieceQuantity
         , dm.Quantity * ISNULL(vuc2.factor, 0) AS WeightPerPieceQuantity
         , dm._RecID
         , dm._SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM tagactuals_factdetailmain              dm
      LEFT JOIN {{ ref('vwuomconversion') }} vuc1
        ON vuc1.legalentitykey = dm.LegalEntityKey
       AND vuc1.productkey     = dm.ProductKey
       AND vuc1.fromuomkey     = dm.UOMKey
       AND vuc1.touomkey       = dm.SumUOMKey
      LEFT JOIN {{ ref('vwuomconversion') }} vuc2
        ON vuc2.legalentitykey = dm.LegalEntityKey
       AND vuc2.productkey     = dm.ProductKey
       AND vuc2.fromuomkey     = dm.UOMKey
       AND vuc2.touomkey       = dm.MassUOMKey;
