{{ config(materialized='table', tags=['silver'], alias='warehouselocation') }}

-- Source file: cma/cma/layers/_base/_silver/warehouselocation/warehouselocation.py
-- Root method: Warehouselocation.warehouselocationdetail [WarehouseLocationDetail]
-- external_table_name: WarehouseLocationDetail
-- schema_name: temp

SELECT  ROW_NUMBER() OVER (ORDER BY wl.recid) AS WarehouseLocationKey
         ,wl.dataareaid                                                 AS LegalEntityID
      , ins.siteid                                                     AS SiteID
      , ISNULL(NULLIF(LTRIM(RTRIM(ins.name)), ''), ins.siteid)         AS Site
      , il.inventlocationid                                                   AS WarehouseID
      , ISNULL(NULLIF(LTRIM(RTRIM(il.name)), ''), il.inventlocationid) AS Warehouse
      , wl.wmslocationid                                                      AS WarehouseLocation
      , wl.recid                                                             AS _RecID
      , 1                                                                     AS _SourceID

      , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                            AS _CreatedDate
      , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                            AS _ModifiedDate  
   FROM {{ ref('wmslocation') }}       wl
INNER JOIN {{ ref('inventlocation') }}  il
   ON il.dataareaid      = wl.dataareaid
   AND il.inventlocationid = wl.inventlocationid
INNER JOIN {{ ref('inventsite') }}     ins
   ON ins.dataareaid     = il.dataareaid
   AND ins.siteid          = il.inventsiteid ;

