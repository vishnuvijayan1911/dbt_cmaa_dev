{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/product/product.py
-- Root method: Product.productdetail [ProductDetail]
-- Inlined methods: Product.productproduct1 [ProductProduct1], Product.productvariant [ProductVariant], Product.productvariantunit [ProductVariantUnit], Product.productdetail1 [ProductDetail1]
-- external_table_name: ProductDetail
-- schema_name: temp

WITH
productproduct1 AS (
    SELECT DISTINCT
               it.dataareaid                                                                     AS DATAAREAID
             , it.itemid                                                                          AS ItemID
             , it.product                                                                         AS ProductID
    		     , erc.name																			                                      AS ProductCategory
             , pt.name                                                                            AS BaseProduct
             , REPLACE (REPLACE (pt.description, CHAR (13), ' '), CHAR (10), ' ')                 AS BaseProductDesc
             , iigi.itemgroupid                                                                   AS ItemGroupID
             , iig.name                                                                           AS ItemGroup
             , imgi.modelgroupid                                                                  AS ItemModelGroupID
             , img.name                                                                           AS ItemModelGroup
             , it.itemtype                                                                        AS ItemTypeID
             , im2.unitid                                                                 AS SalesUOM
             , sdg.name                                                                           AS StorageGroupID
             , sdg.description                                                                    AS StorageGroup
             , tdg.name                                                                           AS TrackingGroupID
             , tdg.description                                                                    AS TrackingGroup
             , im.unitid                                                                  AS InventoryUOM
             , CASE WHEN it.cmarecoverablescrap = 1 THEN 'Recoverable scrap' ELSE 'Not scrap' END AS RecoverableScrap
             , 1                                                                                  AS _SourceID
             , it.recid                                                                           AS _RecID

          FROM {{ ref('inventtable') }}                           it
         INNER JOIN {{ ref('ecoresproduct') }}                    pr 
            ON pr.recid           = it.product
         INNER JOIN {{ ref('inventtablemodule') }}                im
            ON im.dataareaid      = it.dataareaid
           AND im.itemid           = it.itemid
           AND im.moduletype       = 0
          LEFT JOIN {{ ref('inventtablemodule') }}                im2
            ON im2.dataareaid     = it.dataareaid
           AND im2.itemid          = it.itemid
           AND im2.moduletype      = 2 
          LEFT JOIN {{ ref('ecoresproducttranslation') }}         pt
            ON pt.product          = pr.recid
           AND pt.languageid       = 'en-us'
          LEFT JOIN {{ ref('ecorestrackingdimensiongroupitem') }} tdgi
            ON tdgi.itemdataareaid = it.dataareaid
           AND tdgi.itemid         = it.itemid
          LEFT JOIN {{ ref('ecorestrackingdimensiongroup') }}     tdg
            ON tdg.recid          = tdgi.trackingdimensiongroup
          LEFT JOIN {{ ref('ecoresstoragedimensiongroupitem') }}  sdgi
            ON sdgi.itemdataareaid = it.dataareaid
           AND sdgi.itemid         = it.itemid
          LEFT JOIN {{ ref('ecoresstoragedimensiongroup') }}      sdg
            ON sdg.recid          = sdgi.storagedimensiongroup
          LEFT JOIN {{ ref('inventmodelgroupitem') }}             imgi
            ON imgi.itemdataareaid = it.dataareaid
           AND imgi.itemid         = it.itemid
          LEFT JOIN {{ ref('inventmodelgroup') }}                 img
            ON img.dataareaid     = imgi.itemdataareaid
           AND img.modelgroupid    = imgi.modelgroupid
          LEFT JOIN {{ ref('inventitemgroupitem') }}              iigi
            ON iigi.itemdataareaid = it.dataareaid
           AND iigi.itemid         = it.itemid
          LEFT JOIN {{ ref('inventitemgroup') }}                  iig
            ON iig.dataareaid     = iigi.itemdataareaid
           AND iig.itemgroupid     = iigi.itemgroupid
          LEFT JOIN {{ ref('ecoresproductcategory') }} erpc 
            ON pr.recid = erpc.product
          LEFT JOIN {{ ref('ecorescategory') }}  erc 
            ON erpc.category = erc.recid
            WHERE it.dataareaid <> 'DAT'
              AND it.itemid      <> '';
),
productvariant AS (
    SELECT DISTINCT
               prv.recid                                                                      AS ProductID
             , prv.productmaster                                                               AS ProductMasterID
             , pr.dataareaid                                                                  AS DATAAREAID
             , id.configid                                                                     AS ProductConfig
             , id.inventsizeid                                                                 AS ProductWidth
             , id.inventcolorid                                                                AS ProductLength
             , id.inventstyleid                                                                AS ProductColor
             , pt.name                                                                         AS ProductName
             , REPLACE (REPLACE (pt.description, CHAR (13), ' '), CHAR (10), ' ')              AS ProductDesc
               , CAST(ISNULL (NULLIF(TRY_CAST(id.inventcolorid AS FLOAT), 0), 0) AS NUMERIC(32, 16)) AS NumericLength
             , CAST(ISNULL (NULLIF(TRY_CAST(id.inventsizeid AS FLOAT), 0), 0) AS NUMERIC(32, 16))  AS NumericWidth

     FROM productproduct1                        pr
    	INNER join {{ ref('ecoresdistinctproductvariant') }} prv
    		ON prv.productmaster         = pr.productid

          LEFT JOIN {{ ref('ecoresproducttranslation') }} pt
            ON pt.product                = prv.recid
           AND pt.languageid             = 'en-us'
         INNER JOIN {{ ref('inventdimcombination') }}     ic
            ON ic.dataareaid            = pr.dataareaid
           AND ic.distinctproductvariant = prv.recid
         INNER JOIN {{ ref('inventdim') }}                id
            ON id.dataareaid            = ic.dataareaid
           AND id.inventdimid            = ic.inventdimid
),
productvariantunit AS (
    SELECT tv.DATAAREAID
             , tv.ProductMasterID              AS ProductID
             , MAX (eg.name)                   AS DimensionGroup
             , MAX (fs.cmametricunit)  AS LengthUnit
             , MAX (fs.cmanumericunit) AS WidthUnit

          FROM productvariant                                   tv
         INNER JOIN {{ ref('ecoresproductdimensiongroupproduct') }}  ep
            ON ep.product               = tv.ProductMasterID
         INNER JOIN {{ ref('ecoresproductdimensiongroup') }}         eg
            ON eg.recid                = ep.productdimensiongroup
         INNER JOIN {{ ref('ecoresproductdimensiongroupfldsetup') }} fs
            ON fs.productdimensiongroup = eg.recid
           AND fs.isactive              = 1
         GROUP BY tv.DATAAREAID
                , tv.ProductMasterID;
),
productdetail1 AS (
    SELECT 
              p1.BaseProduct                                                              AS BaseProduct
             , p1.BaseProductDesc                                                         AS BaseProductDesc
             , pu.DimensionGroup
             , p1.ItemID                                                                  AS ItemID
             , p1.ItemGroupID
             , p1.ItemGroup
             , p1.ItemModelGroupID
             , p1.ItemModelGroup
             , p1.ItemTypeID                                                              AS ItemTypeID
             , we1.enumvalue                                                              AS ItemType
             , p1.InventoryUOM
             , p1.DATAAREAID                                                             AS LegalEntityID
             , pu.LengthUnit
             , ISNULL (pv.ProductColor, '')                                               AS ProductColor
             , ISNULL (pv.ProductDesc, p1.BaseProductDesc)                                AS ProductDesc
             , ISNULL (pv.ProductID, p1.ProductID)                                        AS ProductID
             , ISNULL (pv.ProductLength, '')                                              AS ProductLength
             , ISNULL (pv.ProductMasterID, 0)                                             AS ProductMasterID
             , CASE WHEN pv.ProductID IS NULL THEN 0 ELSE 2 END                           AS ProductSubTypeID
             , CASE WHEN pv.ProductID IS NULL THEN 'Product' ELSE 'Product variant' END   AS ProductSubType
             , ISNULL (pv.ProductConfig, '')                                              AS ProductConfig
             , ISNULL (pv.ProductWidth, '')                                               AS ProductWidth
             , ISNULL (pv.ProductName, p1.BaseProduct) + ' ' + '-' + ' ' + p1.ItemID AS Product
             , ISNULL (pv.ProductName, p1.BaseProduct)                                    AS ProductName
             , p1.ProductCategory
             , p1.SalesUOM
             , p1.StorageGroupID
             , p1.StorageGroup
             , p1.TrackingGroupID
             , p1.TrackingGroup
             , pu.WidthUnit
             , p1.RecoverableScrap
             , p1._RecID
             , p1._SourceID
          FROM productproduct1            p1
          LEFT JOIN productvariant        pv
            ON pv.DATAAREAID     = p1.DATAAREAID
           AND pv.ProductMasterID = p1.ProductID
          LEFT JOIN productvariantunit    pu
            ON pu.DATAAREAID     = p1.DATAAREAID
           AND pu.ProductID       = p1.ProductID
          LEFT JOIN {{ ref('enumeration') }} we1
            ON we1.enum           = 'ItemType'
           AND we1.enumvalueid    = p1.ItemTypeID;
)
SELECT 
         ROW_NUMBER() OVER (ORDER BY pv._RecID, pv._SourceID) AS ProductKey 
         ,
         pv.DimensionGroup
         , pv.ItemID
         , pv.ItemGroupID
         , CASE WHEN pv.ItemGroup = '' THEN pv.ItemGroupID ELSE pv.ItemGroup END                AS ItemGroup
         , pv.ItemModelGroupID
         , CASE WHEN pv.ItemModelGroup = '' THEN pv.ItemModelGroupID ELSE pv.ItemModelGroup END AS ItemModelGroup
         , pv.ItemTypeID
         , CASE WHEN pv.ItemType = '' THEN CAST(pv.ItemTypeID AS VARCHAR)ELSE pv.ItemType END   AS ItemType
         , pv.InventoryUOM
         , pv.LegalEntityID
         , pv.LengthUnit
         , pv.ProductColor
         , pv.ProductDesc
         , pv.ProductID
         , pv.ProductLength
         , pv.ProductMasterID
         , pv.ProductSubTypeID
         , pv.ProductSubType
         , pv.ProductConfig
         , pv.ProductWidth
         , pv.Product
         , CASE WHEN pv.ProductName = '' THEN pv.Product ELSE pv.ProductName END                AS ProductName
         , pv.ProductCategory
         , pv.StorageGroupID
         , CASE WHEN pv.StorageGroup = '' THEN pv.StorageGroupID ELSE pv.StorageGroup END       StorageGroup
         , CASE WHEN pv.TrackingGroup = '' THEN pv.TrackingGroupID ELSE pv.TrackingGroup END    TrackingGroup
         , pv.TrackingGroupID
         , pv.WidthUnit
         , pv.RecoverableScrap
         , pv._SourceID
         , pv._RecID
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
         ,CURRENT_TIMESTAMP                                               AS _ModifiedDate
         ,'1900-01-01'                                               AS ActivityDate


      FROM productdetail1               pv

          UNION 

	   SELECT 
	   -1 AS ProductKey 
         ,
         '' AS DimensionGroup
         , '' AS ItemID
         , '' AS ItemGroupID
         , '' AS  ItemGroup
         , '' AS ItemModelGroupID
         , '' AS  ItemModelGroup
         , 0 AS ItemTypeID
         , '' AS  ItemType
         , '' AS InventoryUOM
         , '' AS LegalEntityID
         , '' AS LengthUnit
         , '' AS ProductColor
         , '' AS ProductDesc
         , 0 AS ProductID
         , '' AS ProductLength
         , 0 AS ProductMasterID
         , 0 AS ProductSubTypeID
         , '' AS ProductSubType
         , '' AS ProductConfig
         , '' AS ProductWidth
         , '' AS Product
         , '' AS  ProductName
         , '' AS ProductCategory
         , '' AS StorageGroupID
         , '' AS        StorageGroup
         , '' AS    TrackingGroup
         , '' AS TrackingGroupID
         , '' AS WidthUnit
         , '' AS RecoverableScrap
         , 0 AS _SourceID
         , 0 AS _RecID
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
         ,CURRENT_TIMESTAMP                                               AS _ModifiedDate
         ,'1900-01-01'                                               AS ActivityDate
