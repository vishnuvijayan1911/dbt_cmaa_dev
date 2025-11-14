{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/warehouse/warehouse.py
-- Root method: Warehouse.get_detail_query [WarehouseDetail]
-- Inlined methods: Warehouse.get_query_for_warehouse_stage [WarehouseStage], Warehouse.get_query_for_warehouse_address [WarehouseAddress]
-- external_table_name: WarehouseDetail
-- schema_name: temp

WITH
warehousestage AS (
    SELECT il.dataareaid                                                        AS LegalEntityID
        , il.cmacustaccount                                                     AS CustomerAccount
        , ll.location                                                           AS Location
        , ins.siteid                                                            AS SiteID
        , ISNULL(NULLIF(LTRIM(RTRIM(ins.name)), ''), ins.siteid)         AS Site
        , il.inventlocationid                                                   AS WarehouseID
        , ISNULL(NULLIF(LTRIM(RTRIM(il.name)), ''), il.inventlocationid) AS Warehouse
        , il.inventlocationtype                                                 AS WarehouseTypeID
        , il.vendaccount                                                        AS VendorAccount
        , il.modifieddatetime                                                   AS _SourceDate
        , il.recid                                                              AS _RecID
        , 1                                                                     AS _SourceID

      FROM {{ ref('inventlocation') }}                     il
      INNER JOIN  {{ ref('inventsite') }}                     ins
        ON ins.dataareaid   = il.dataareaid 
      AND ins.siteid        = il.inventsiteid
      LEFT JOIN {{ ref('inventlocationlogisticslocation') }} ll
        ON ll.inventlocation = il.recid
      AND ll.isprimary      = 1
    WHERE il.inventlocationid <> '';
),
warehouseaddress AS (
    SELECT t.*
      FROM (   SELECT Location
                    , Street    
                    , City
                    , StateProvince
                    , PostalCode
                    , CountryID
                    , Country
                    , _RecID
                    , ROW_NUMBER() OVER (PARTITION BY Location
    ORDER BY _RecID DESC) AS Rank_Val
                FROM silver.cma_Address) t
    WHERE t.Rank_Val = 1;
)
SELECT 
 ROW_NUMBER() OVER (ORDER BY ts.WarehouseID, ts.LegalEntityID) AS WarehouseKey
     , ts.LegalEntityID                                                                    AS LegalEntityID
     , ts.CustomerAccount                                                                  AS CustomerAccount
     , ts.SiteID                                                                           AS SiteID
     , CASE WHEN ts.Site = '' THEN ts.SiteID ELSE ts.Site END                              AS Site
     , ts.WarehouseID                                                                      AS WarehouseID
     , CASE WHEN ts.Warehouse = '' THEN ts.WarehouseID ELSE ts.Warehouse END               AS Warehouse
     , ts.WarehouseTypeID                                                                  AS WarehouseTypeID
     , CASE we1.enumvalue WHEN 'Default' THEN 'Standard' ELSE we1.enumvalue END            AS WarehouseType
     , lpa.Street                                                                          AS WarehouseStreet
     , lpa.City                                                                            AS WarehouseCity
     , lpa.StateProvince                                                                   AS WarehouseStateProvince
     , CASE WHEN lpa.CountryID = 'USA' THEN LEFT(lpa.PostalCode, 5)ELSE lpa.PostalCode END AS WarehousePostalCode
     , lpa.CountryID                                                                       AS WarehouseCountryID
     , CASE WHEN lpa.Country = '' THEN lpa.CountryID ELSE lpa.Country END                  AS WarehouseCountry
     , ts.VendorAccount                                                                    AS VendorAccount
     , ts._SourceDate                                                                      AS _SourceDate
     , ts._RecID                                                                           AS _RecID
     , ts._SourceID                                                                        AS _SourceID
     , CURRENT_TIMESTAMP                                                                   AS _CreatedDate
     , CURRENT_TIMESTAMP                                                                   AS _ModifiedDate  

   FROM warehousestage               ts
   LEFT JOIN warehouseaddress        lpa
     ON lpa.Location    = ts.Location
   LEFT JOIN {{ ref('enumeration') }} we1
     ON we1.enum        = 'InventLocationType'
   AND we1.enumvalueid = ts.WarehouseTypeID
