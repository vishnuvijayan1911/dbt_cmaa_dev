{{ config(materialized='table', tags=['silver'], alias='lot_fact') }}

-- Source file: cma/cma/layers/_base/_silver/lot_f/lot_f.py
-- Root method: LotFact.lot_factdetail [Lot_FactDetail]
-- Inlined methods: LotFact.lot_factstage [Lot_FactStage]
-- external_table_name: Lot_FactDetail
-- schema_name: temp

WITH
lot_factstage AS (
    SELECT ito.dataareaid       AS LegalEntityID
             , id.configid           AS ProductConfig
             , id.inventcolorid      AS ProductLength
             , id.inventstyleid      AS ProductColor
             , id.inventsizeid       AS ProductWidth
             , ito.itemid            AS ItemID
             , ito.referencecategory AS REFERENCECATEGORY
             , ito.recid            AS _RecID

          FROM {{ ref('inventtransorigin') }} ito
         INNER JOIN {{ ref('inventdim') }}    id
            ON id.dataareaid = ito.dataareaid
           AND id.inventdimid = ito.iteminventdimid;
)
SELECT di.LotKey              AS LotKey
         , le.LegalEntityKey      AS LegalEntityKey
         , dis.InventorySourceKey AS InventorySourceKey
         , dp.ProductKey          AS ProductKey
         , 1                      AS _SourceID
         , ts._RecID              AS _RecID
         ,  CURRENT_TIMESTAMP  AS  _CreatedDate
         , CURRENT_TIMESTAMP AS _ModifiedDate

      FROM lot_factstage                  ts
     INNER JOIN {{ ref('legalentity_d') }}     le
        ON le.LegalEntityID      = ts.LegalEntityID
     INNER JOIN {{ ref('lot_d') }}             di
        ON di._RecID             = ts._RecID
       AND di._SourceID          = 1
      LEFT JOIN {{ ref('inventorysource_d') }} dis
        ON dis.InventorySourceID = ts.REFERENCECATEGORY
      LEFT JOIN {{ ref('product_d') }}         dp
        ON dp.LegalEntityID      = ts.LegalEntityID
       AND dp.ItemID             = ts.ItemID
       AND dp.ProductLength      = ts.ProductLength
       AND dp.ProductColor       = ts.ProductColor
       AND dp.ProductWidth       = ts.ProductWidth
       AND dp.ProductConfig      = ts.ProductConfig;
