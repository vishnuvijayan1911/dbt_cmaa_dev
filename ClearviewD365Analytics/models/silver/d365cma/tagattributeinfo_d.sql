{{ config(materialized='table', tags=['silver'], alias='tagattributeinfo') }}

-- Source file: cma/cma/layers/_base/_silver/tagattributeinfo/tagattributeinfo.py
-- Root method: TagAttributeInfo.tagattributeinfodetail [TagAttributeInfoDetail]
-- Inlined methods: TagAttributeInfo.cmaproductattributes [CMAProductAttributes], TagAttributeInfo.tagattributeinfodetail1 [TagAttributeInfoDetail1]
-- external_table_name: TagAttributeInfoDetail
-- schema_name: temp

WITH
cmaproductattributes AS (
    SELECT it.itemid   AS ItemID
           ,it.dataareaid as DataAreaID
           , dim1.value      AS [Dimension 1]
           , dim2.value      AS [Dimension 2]
           , Grade.value      AS [Grade]
           , Shape.value      AS [Shape]
           , CONCAT (pt.name, ' ', '-', ' ', it.itemid) AS [Product]
        FROM {{ ref('inventtable') }}  it
        LEFT JOIN {{ ref('productattributevalues') }} dim1 
        ON it.product = dim1.productid
        and dim1.name   ='Dimension 1'
        LEFT JOIN {{ ref('productattributevalues') }} dim2 
        ON it.product = dim2.productid
        and dim2.name   ='Dimension 2'
        LEFT JOIN {{ ref('productattributevalues') }} Grade 
        ON it.product = Grade.productid
        and Grade.name   ='Grade'
        LEFT JOIN {{ ref('productattributevalues') }} Shape 
        ON it.product = Shape.productid
        and Shape.name   ='Shape'
        LEFT JOIN {{ ref('ecoresproducttranslation') }}  pt
        ON pt.product    = it.product
        AND pt.languageid = 'en-us';
),
tagattributeinfodetail1 AS (
    SELECT a.dataareaid 
            , a.inventbatchid 
            , a.itemid
            , MAX(CASE WHEN TRIM(PDSBATCHATTRIBID) = 'Melt Mill Source' THEN TRIM(PDSBATCHATTRIBVALUE) END) AS [Melt mill source]
            , MAX(CASE WHEN TRIM(PDSBATCHATTRIBID) = 'Roll Mill Source' THEN TRIM(PDSBATCHATTRIBVALUE) END) AS [Roll mill source]
         FROM {{ ref('pdsbatchattributes') }} a
        WHERE TRIM(PDSBATCHATTRIBID) IN ('Melt Mill Source','Roll Mill Source')
        Group by a.dataareaid 
            , a.inventbatchid 
            , a.itemid
)
SELECT ROW_NUMBER () OVER (ORDER BY t._RECID) AS TagAttributeInfoKey,
       *
  FROM (
          SELECT a.dataareaid + ' - ' + da.name AS [Legal entity]
         , a.inventbatchid            AS [Tag #]
         , p.cmaheatnumber            AS [Heat #]
         , ta.[Melt mill source]
         , ta.[Roll mill source]
         , a.itemid                   AS [Item #]
         , pa.[Dimension 1]
         , pa.[Dimension 2]
         , pa.Grade                  AS [Grade]
         , pa.Shape                  AS [Shape]
         , pa.Product                AS [Product]
         , TRIM(PDSBATCHATTRIBID)    AS [Tag attribute name]
         , TRIM(PDSBATCHATTRIBVALUE) AS [Tag attribute value]
         , e.enumvalue               AS [Tag attribute category]
         , CAST(p.proddate AS DATE)  AS [Created date time]
         , a.recid                   AS [_RecID]
      FROM {{ ref('pdsbatchattributes') }} a
      INNER JOIN {{ ref('dataarea') }} da
         ON da.fno_id = a.dataareaid
      INNER JOIN {{ ref('inventbatch') }}  p
      on a.inventbatchid = p.inventbatchid
      and a.itemid = p.itemid
      and a.dataareaid = p.dataareaid
      INNER JOIN {{ ref('enumeration') }} e
         ON e.enumvalueid = a.cmabacategorysort
         AND e.enum = 'BACategorySort'
      LEFT JOIN cmaproductattributes pa
      on pa.ItemID = a.itemid
      and pa.DataAreaID = a.dataareaid
      LEFT JOIN tagattributeinfodetail1 ta
      on ta.ItemID = a.itemid
      and ta.DataAreaID = a.dataareaid
      and ta.Inventbatchid = a.inventbatchid
     WHERE TRIM(PDSBATCHATTRIBVALUE) <> ''
     and e.enumvalue IN ('Chemical', 'Mechanical','Physical','Cleanliness','Hardenability','ASTM E45 Method A (Worst)','ASTM E45 Method A (Average)','ASTM 345 Method E','ASTM E45 Method C','ASTM E381', 'Bundle Length', 'Other', 'Diameter')
--and 1=0
) t

