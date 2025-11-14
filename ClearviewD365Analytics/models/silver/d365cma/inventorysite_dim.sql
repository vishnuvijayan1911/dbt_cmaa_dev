{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/inventorysite/inventorysite.py
-- Root method: Inventorysite.inventorysitedetail [InventorySiteDetail]
-- external_table_name: InventorySiteDetail
-- schema_name: temp

SELECT 
     ROW_NUMBER() OVER (ORDER BY ivs.recid) AS InventorySiteKey,
    ivs.dataareaid                                            AS LegalEntityID
         , ivs.siteid                                                AS InventorySiteID
         , CASE WHEN ivs.name = '' THEN ivs.siteid ELSE ivs.name END AS InventorySite
         , ivs.recid                                                 AS _RecID
         , 1                                                         AS _SourceID
        ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM {{ ref('inventsite') }} ivs
     WHERE ivs.siteid <> ''
