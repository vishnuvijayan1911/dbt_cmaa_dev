{{ config(materialized='table', tags=['silver'], alias='qualityorderline') }}

-- Source file: cma/cma/layers/_base/_silver/qualityorderline/qualityorderline.py
-- Root method: Qualityorderline.qualityorderlinedetail [QualityOrderLineDetail]
-- external_table_name: QualityOrderLineDetail
-- schema_name: temp

SELECT ROW_NUMBER () OVER (ORDER BY ts.LegalEntityID, ts.QualityOrderID, ts.SequenceNum) AS QualityOrderLineKey
     , ts.*
  FROM (   SELECT t.qualityorderid                                                       AS QualityOrderID
                , t.dataareaid                                                           AS LegalEntityID
                , RIGHT('000' + CAST (CAST (t.testsequence AS BIGINT) AS VARCHAR(6)), 6) AS SequenceNum
                , t.pdsbatchattribid                                                     AS TagAttributeID
                , t.pdsattribvalue                                                       AS TagAttributeValue
                , t.testinstrumentid                                                     AS TestInstrumentID
                , t.variableid                                                           AS Variable
                , t.variableoutcomeidstandard                                            AS DefaultOutcome
                , CASE WHEN t.cmaiscalculated = 1 THEN 'Yes' ELSE 'No' END               AS IsCalculated
                , CASE WHEN t.testresult = 1 THEN 'Yes' ELSE 'No' END                    AS IsIncludedInResult
                , CASE WHEN t.includeresults = 1 THEN 'Yes' ELSE 'No' END                AS IsIncludeResults
                , CASE WHEN t.testresult = 1 THEN 'Yes' ELSE 'No' END                    AS IsTestResult
                , t.createdby                                                            AS CreatedBy
                , t.modifiedby                                                           AS ModifiedBy
                , t.recid                                                                AS _RecID
                , 1                                                                      AS _SourceID
                , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
                , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
             FROM {{ ref('inventqualityorderline') }} t) ts;

