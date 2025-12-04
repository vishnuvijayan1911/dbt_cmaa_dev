{{ config(materialized='view', schema='gold', alias="Warehouse") }}

SELECT t.WarehouseKey                        AS [Warehouse key]
     , CONCAT (t.LegalEntityID, t.SiteID)    AS [Inventory site index]
     , NULLIF (t.LegalEntityID, '')          AS [Legal entity]
     , NULLIF (t.CustomerAccount, '')        AS [Customer #]
     , NULLIF (t.SiteID, '')                 AS [Inventory site]
     , NULLIF (t.Site, '')                   AS [Inventory site name]
     , NULLIF (t.WarehouseID, '')            AS [Warehouse]
     , NULLIF (t.WarehouseCity, '')          AS [Warehouse city]
     , NULLIF (t.WarehouseCountryID, '')     AS [Warehouse country]
     , NULLIF (t.WarehouseCountry, '')       AS [Warehouse country name]
     , NULLIF (t.Warehouse, '')              AS [Warehouse name]
     , NULLIF (t.WarehouseStateProvince, '') AS [Warehouse state province]
     , NULLIF (t.WarehouseStreet, '')        AS [Warehouse street]
     , NULLIF (t.WarehouseType, '')          AS [Warehouse type]
     , NULLIF (t.WarehousePostalCode, '')    AS [Warehouse postal code]
     , NULLIF (t.VendorAccount, '')          AS [Vendor #]
  FROM {{ ref("d365cma_warehouse_d") }} t;
