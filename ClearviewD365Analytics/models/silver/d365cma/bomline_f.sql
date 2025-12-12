{{ config(materialized='table', tags=['silver'], alias='bomline_fact') }}

-- Source file: cma/cma/layers/_base/_silver/bomline_f/bomline_f.py
-- Root method: BomlineFact.bomline_factdetail [BOMLine_FactDetail]
-- Inlined methods: BomlineFact.bomline_factstage [BOMLine_FactStage], BomlineFact.bomline_factbom [BOMLine_FactBOM]
-- external_table_name: BOMLine_FactDetail
-- schema_name: temp

WITH
bomline_factstage AS (
    SELECT b.dataareaid                                                  AS LegalEntityID
             , b.bomid                                                         AS BOMID
             , b.bomqty                                                        AS BOMQuantity
             , b.itemid                                                        AS ItemID
             , RIGHT('000' + CAST(CAST(b.linenum AS BIGINT) AS VARCHAR(6)), 6) AS LineNumber
             , id.inventstyleid                                                AS ProductColor
             , id.inventcolorid                                                AS ProductLength
             , id.inventsizeid                                                 AS ProductWidth
             , id.configid                                                     AS ProductConfig
             , b.unitid                                                        AS UnitID
             , b.recid                                                        AS _RecID
             , 1                                                               AS _SourceID
          FROM {{ ref('bom') }}            b
          LEFT JOIN {{ ref('inventdim') }} id
            ON id.dataareaid = b.dataareaid
           AND id.inventdimid = b.inventdimid;
),
bomline_factbom AS (
    SELECT t.*

          FROM (   SELECT db.*
                        , ROW_NUMBER() OVER (PARTITION BY db.LegalEntityID, db.BOMID
                                                 ORDER BY db._RecID DESC) AS RankVal
                     FROM {{ ref('bom') }}      b
                    INNER JOIN {{ ref('bom_d') }} db
                       ON db.LegalEntityID = b.dataareaid
                      AND db.BOMID         = b.bomid) t
         WHERE t.RankVal = 1;
)
SELECT {{ dbt_utils.generate_surrogate_key(['ts._RecID', 'ts._SourceID']) }} AS BOMLineKey
         , le.LegalEntityKey AS LegalEntityKey
         , tb.BOMKey         AS BOMKey
         , dp.ProductKey     AS ProductKey
         , du.UOMKey         AS BOMUOMKey
         , ts.BOMQuantity    AS BOMQuantity
         , ts.LineNumber     AS LineNumber
         , ts._RecID         AS _RecID
         , ts._SourceID      AS _SourceID
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM bomline_factstage              ts
     INNER JOIN {{ ref('legalentity_d') }} le
        ON le.LegalEntityID = ts.LegalEntityID
      LEFT JOIN bomline_factbom           tb
        ON tb.LegalEntityID = ts.LegalEntityID
       AND tb.BOMID         = ts.BOMID
      LEFT JOIN {{ ref('product_d') }}     dp
        ON dp.LegalEntityID = ts.LegalEntityID
       AND dp.ItemID        = ts.ItemID
       AND dp.ProductLength = ts.ProductLength
       AND dp.ProductColor  = ts.ProductColor
       AND dp.ProductWidth  = ts.ProductWidth
       AND dp.ProductConfig = ts.ProductConfig
      LEFT JOIN {{ ref('uom_d') }}         du
        ON du.UOM           = ts.UnitID;
