{{ config(materialized='table', tags=['silver'], alias='productionbom') }}

-- Source file: cma/cma/layers/_base/_silver/productionbom/productionbom.py
-- Root method: Productionbom.productionbomdetail [ProductionBOMDetail]
-- Inlined methods: Productionbom.productionbomstage [ProductionBOMStage]
-- external_table_name: ProductionBOMDetail
-- schema_name: temp

WITH
productionbomstage AS (
    SELECT pb.dataareaid                                                       AS LegalEntityID
             , pb.prodflushingprincip                                              AS FlushingPrincipalID
             , RIGHT('000' + CAST(CAST(pb.linenum AS BIGINT) AS VARCHAR(6)), 6)    AS LineNumber
             , pb.prodlinetype                                                     AS LineTypeID
             , pb.oprnum                                                           AS OperationNumber
             , pb.bomconsump                                                       AS ProductConsumedID
             , pb.prodid                                                           AS ProductionID
             , pb.unitid                                                    AS ProductionUOM
             , pb.inventrefid                                                      AS ReferenceID
             , pb.inventreftype                                                    AS ReferenceTypeID
             , pb.inventreftransid                                                 AS TransReference
             , pb.cmarecoverablescrap                                              AS IsRecoverableScrap
             , pb.cmareturntostock                                                 AS IsRTS
             , pb.cmacalcyield                                                     AS IsBOQA
             , CASE WHEN pb.prodlinetype = 3 AND ib.itemtype = 2 THEN 1 ELSE 0 END AS IsOSP
             , pb.recid                                                            AS _RecID
             , 1                                                                   AS _SourceID

          FROM {{ ref('prodbom') }}          pb
          LEFT JOIN {{ ref('inventtable') }} ib
            ON ib.dataareaid  = pb.dataareaid
           AND ib.itemid      = pb.itemid;
)
SELECT
          ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS ProductionBOMKey
         ,ts.LegalEntityID      AS LegalEntityID
         , ts.LineNumber         AS LineNumber
         , ts.OperationNumber    AS OperationNumber
         , ts.ProductionUOM      AS ProductionUOM
         , ts.ProductionID       AS ProductionID
         , ts.ReferenceID        AS ReferenceID
         , ts.TransReference     AS TransReference
         , ts.IsRecoverableScrap AS IsRecoverableScrap
         , ts.IsRTS              AS IsRTS
         , ts.IsBOQA             AS IsBOQA
         , ts.IsOSP              AS IsOSP
         , ts._RecID             AS _RecID
         , ts._SourceID          AS _SourceID
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate


    FROM productionbomstage               ts
     INNER JOIN {{ ref('legalentity_d') }} le
        ON le.LegalEntityID = ts.LegalEntityID
      LEFT JOIN {{ ref('enumeration') }} e1
        ON e1.enum          = 'ProdFlushingPrincipBOM'
       AND e1.enumvalueid   = ts.FlushingPrincipalID
      LEFT JOIN {{ ref('enumeration') }} e2
        ON e2.enum          = 'BOMType'
       AND e2.enumvalueid   = ts.LineTypeID
      LEFT JOIN {{ ref('enumeration') }} e3
        ON e3.enum          = 'BOMConsumpType'
       AND e3.enumvalueid   = ts.ProductConsumedID
      LEFT JOIN {{ ref('enumeration') }} e4
        ON e4.enum          = 'InventRefType'
       AND e4.enumvalueid   = ts.ReferenceTypeID;

