{{ config(materialized='table', tags=['silver'], alias='qualityorderline_fact') }}

-- Source file: cma/cma/layers/_base/_silver/qualityorderline_f/qualityorderline_f.py
-- Root method: QualityorderlineFact.qualityorderline_factdetail [QualityOrderLine_FactDetail]
-- Inlined methods: QualityorderlineFact.qualityorderline_factstage [QualityOrderLine_FactStage]
-- external_table_name: QualityOrderLine_FactDetail
-- schema_name: temp

WITH
qualityorderline_factstage AS (
    SELECT t.qualityorderid         AS QualityOrderID
         , t.dataareaid             AS LegalEntityID
         , t.testid                 AS TestID
         , t.cmaoprid               AS OperationID
         , t.testunitid             AS TestUOM
         , t.acceptablequalitylevel AS AcceptableQualityLevel
         , t.lowerlimit             AS MinLimit
         , t.lowertolerance         AS MinTolerance
         , t.upperlimit             AS MaxLimit
         , t.uppertolerance         AS MaxTolerance
         , t.standardvalue          AS StandardValue
         , t.pdsorderlineresult     AS TestValue
         , t.recid                  AS RecID
      FROM {{ ref('inventqualityorderline') }} t;
)
SELECT dqol.QualityOrderLineKey        AS QualityOrderLineKey
     , le.LegalEntityKey               AS LegalEntityKey
     , pro.ProductionRouteOperationKey AS ProductionRouteOperationKey
     , dqo.QualityOrderKey             AS QualityOrderKey
     , dt.QualityTestKey               AS TestKey
     , uom.UOMKey                      AS TestUOMKey
     , ts.AcceptableQualityLevel       AS AcceptableQualityLevel
     , ts.MinLimit                     AS MinLimit
     , ts.MinTolerance                 AS MinTolerance
     , ts.MaxLimit                     AS MaxLimit
     , ts.MaxTolerance                 AS MaxTolerance
     , ts.StandardValue                AS StandardValue
     , ts.TestValue                    AS TestValue
     , ts.RecID                        AS _RecID
     , 1                               AS _SourceID
  FROM qualityorderline_factstage      ts
 INNER JOIN silver.cma_QualityOrderLine         dqol
    ON dqol._RecID        = ts.RecID
   AND dqol._SourceID     = 1
  LEFT JOIN silver.cma_LegalEntity              le
    ON le.LegalEntityID   = ts.LegalEntityID
  LEFT JOIN silver.cma_QualityOrder             dqo
    ON dqo.LegalEntityID  = ts.LegalEntityID
   AND dqo.QualityOrderID = ts.QualityOrderID
  LEFT JOIN silver.cma_ProductionRouteOperation pro
    ON pro.LegalEntityID  = ts.LegalEntityID
   AND pro.OperationID    = ts.OperationID
  LEFT JOIN silver.cma_QualityTest                     dt
    ON dt.LegalEntityID   = ts.LegalEntityID
   AND dt.TestID          = ts.TestID
  LEFT JOIN silver.cma_UOM                      uom
    ON uom.UOM            = ts.TestUOM;
