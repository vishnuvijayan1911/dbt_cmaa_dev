{{ config(materialized='table', tags=['silver'], alias='salescategory') }}

-- Source file: cma/cma/layers/_base/_silver/salescategory/salescategory.py
-- Root method: Salescategory.salescategorydetail [SalesCategoryDetail]
-- external_table_name: SalesCategoryDetail
-- schema_name: temp

SELECT  {{ dbt_utils.generate_surrogate_key(['ec.recid']) }} AS SalesCategoryKey
       , ec.name                    AS SalesCategory
      , ISNULL(ec2.name, ec1.name) AS ProductFamily
      , ec1.name                   AS ProductCategory
      , ec.recid                 AS _RecID
      , 1                          AS _SourceID
      ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
  , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
   FROM {{ ref('ecorescategory') }}                 ec
INNER JOIN  {{ ref('ecorescategoryhierarchy') }}     ech
   ON ech.recid                      = ec.categoryhierarchy
INNER JOIN {{ ref('ecorescategoryhierarchyrole') }}  echr
   ON echr.categoryhierarchy          = ech.recid
   AND echr.namedcategoryhierarchyrole = 3 
   LEFT JOIN  {{ ref('ecorescategory') }}              ec1
   ON ec1.recid                     = ec.parentcategory
   LEFT JOIN  {{ ref('ecorescategory') }}               ec2
   ON ec2.recid                      = ec1.parentcategory ;

