{{ config(materialized='table', tags=['silver'], alias='asset') }}

-- Source file: cma/cma/layers/_base/_silver/asset/asset.py
-- Root method: Asset.assetdetail [AssetDetail]
-- Inlined methods: Asset.assetstage [AssetStage]
-- external_table_name: AssetDetail
-- schema_name: temp

WITH
assetstage AS (
    SELECT OT.dataareaid                                                         AS LegalEntityID
             , OT.objectid                                                            AS AssetID
             , ISNULL (NULLIF(OT.name, ''), OT.objectid)                              AS Asset
             , AOT.objecttypeid                                                       AS AssetType
             , OT.modelyear                                                           AS ModelYear
             , M.modelid                                                              AS Model
             , OLS.objectlifecyclestateid                                             AS AssetState
             , OT.fixedassetid                                                        AS FixedAssetID
             , OT.wrkctrid                                                            AS ResourceID
             , FL.functionallocationid                                                AS FunctionalLocationID
             , CAST(OT.activefrom AS DATE)                                            AS AssetValidFrom
             , ISNULL (NULLIF(CAST(OT.activeto AS DATE), '1900-01-01'), '9999-12-31') AS AssetValidTo
             , OT.recid                                                            AS _RecID
             , 1                                                                      AS _SourceID

          FROM {{ ref('entassetobjecttable') }}               OT

          LEFT JOIN {{ ref('entassetobjectlifecyclestate') }} OLS
            ON OLS.recid = OT.objectlifecyclestate
          LEFT JOIN {{ ref('entassetfunctionallocation') }}   FL
            ON FL.recid  = OT.functionallocation
          LEFT JOIN {{ ref('entassetobjecttype') }}           AOT
            ON AOT.recid = OT.objecttype
          LEFT JOIN {{ ref('entassetmodel') }}                M
            ON M.recid   = OT.model;
)
SELECT ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS AssetKey
          , afl.AssetFunctionalLocationKey
         , ts.LegalEntityID
         , ts.AssetID
         , ts.Asset
         , ts.AssetState
         , ts.FixedAssetID
		 , ts.ResourceID
         , ts.AssetType
         , ts.Model
         , ts.ModelYear
         , ts.AssetValidFrom
         , ts.AssetValidTo
         , ts._RecID
         , ts._SourceID
         ,  CURRENT_TIMESTAMP    AS  _CreatedDate
         ,  CURRENT_TIMESTAMP    AS  _ModifiedDate

      FROM assetstage                           ts
      LEFT JOIN silver.cma_AssetFunctionalLocation afl
        ON afl.LegalEntityID        = ts.LegalEntityID
       AND afl.FunctionalLocationID = ts.FunctionalLocationID;

