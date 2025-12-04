{{ config(materialized='table', tags=['silver'], alias='inventorysite') }}

-- Source file: cma/cma/layers/_base/_silver/inventorysite/inventorysite.py
-- Root method: Inventorysite.inventorysitedetail [InventorySiteDetail]
-- external_table_name: InventorySiteDetail
-- schema_name: temp

SELECT 
     {{ dbt_utils.generate_surrogate_key(['ivs.recid']) }} AS InventorySiteKey,
    ivs.dataareaid                                            AS LegalEntityID
         , ivs.siteid                                                AS InventorySiteID
         , CASE WHEN ivs.name = '' THEN ivs.siteid ELSE ivs.name END AS InventorySite
         , ivs.recid                                                 AS _RecID
         , 1                                                         AS _SourceID

        ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM {{ ref('inventsite') }} ivs
     WHERE ivs.siteid <> ''

