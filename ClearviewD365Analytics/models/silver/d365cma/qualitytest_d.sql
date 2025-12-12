{{ config(materialized='table', tags=['silver'], alias='qualitytest') }}

-- Source file: cma/cma/layers/_base/_silver/qualitytest/qualitytest.py
-- Root method: QualityTest.qualitytestdetail [QualityTestDetail]
-- external_table_name: QualityTestDetail
-- schema_name: temp

SELECT ROW_NUMBER () OVER (ORDER BY ts._RecID, ts._SourceID) AS QualityTestKey
     , ts.*
  FROM (   SELECT t.testid                                                 AS TestID
                , t.dataareaid                                             AS LegalEntityID
                , t.description                                            AS TestDesc
                , we1.enumvalue                                            AS TestType
                , t.cmatestcategoryid                                      AS TestCategory
                , t.testinstrumentid                                       AS TestInstrument
                , t.testunitid                                             AS TestUnitID
                , t.cmapdsbatchattribid                                    AS TagAttributeID
                , CASE WHEN t.cmaiscalculated = 1 THEN 'Yes' ELSE 'No' END AS IsCalculated
                , t.createdby                                              AS CreatedBy
                , t.modifiedby                                             AS ModifiedBy
                , t.recid                                                  AS _RecID
                , 1                                                        AS _SourceID
                , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
                , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
             FROM {{ ref('inventtesttable') }}  t
             LEFT JOIN {{ ref('enumeration') }} we1
               ON we1.enum        = 'InventTestType'
              AND we1.enumvalueid = t.testtype) ts;

