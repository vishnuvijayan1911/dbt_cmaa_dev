{{ config(materialized='table', tags=['silver'], alias='salesperson') }}

-- Source file: cma/cma/layers/_base/_silver/salesperson/salesperson.py
-- Root method: Salesperson.salespersondetail [SalesPersonDetail]
-- external_table_name: SalesPersonDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY hcm.recid) AS SalesPersonKey
    ,hcm.personnelnumber                                                AS SalesPersonID
			 , ISNULL(
      UPPER(LEFT(dpt.name, 1))
      + CASE WHEN CHARINDEX(' ', dpt.name) > 0
             THEN UPPER(SUBSTRING(dpt.name, CHARINDEX(' ', dpt.name) + 1, 1))
             ELSE UPPER(RIGHT(dpt.name, CASE LEN(dpt.name) WHEN 1 THEN '' WHEN 2 THEN 1 ELSE 2 END)) END
    , '')                                                            AS SalesPersonInitials
         , CASE WHEN dpt.name = '' THEN hcm.personnelnumber ELSE dpt.name END AS SalesPerson
         , hcm.modifieddatetime                                              AS _SourceDate
         , hcm.recid                                                         AS _RecID
         , 1                                                                  AS _SourceID
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                 AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                 AS _ModifiedDate  
      FROM {{ ref('hcmworker') }}          hcm
      LEFT JOIN  {{ ref('dirpartytable') }}  dpt
        ON dpt.recid = hcm.person;

