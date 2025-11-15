{{ config(materialized='table', tags=['silver'], alias='procurementcategory_dim') }}

-- Source file: cma/cma/layers/_base/_silver/procurementcategory/procurementcategory.py
-- Root method: Procurementcategory.procurementcategorydetail [ProcurementCategoryDetail]
-- external_table_name: ProcurementCategoryDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY ec.recid) AS ProcurementCategoryKey
         , ec.name                    AS ProcurementCategory
         , ISNULL(ec2.name, ec1.name) AS ProductFamily
         , ec1.name                   AS ProductCategory
         , 1                          AS _SourceID
         , ec.recid                   AS _RecID
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM {{ ref('ecorescategory') }}       ec
     INNER JOIN  {{ ref('ecorescategoryhierarchy') }}     ech
        ON ech.recid                       = ec.categoryhierarchy
     INNER JOIN  {{ ref('ecorescategoryhierarchyrole') }}  echr
        ON echr.categoryhierarchy          = ech.recid
       AND echr.namedcategoryhierarchyrole = 1 
      LEFT JOIN  {{ ref('ecorescategory') }}              ec1
        ON ec1.recid                       = ec.parentcategory
      LEFT JOIN  {{ ref('ecorescategory') }}               ec2
        ON ec2.recid                       = ec1.parentcategory
