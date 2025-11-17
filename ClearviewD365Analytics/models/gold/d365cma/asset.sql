{{ config(materialized='view', schema='gold', alias="Asset") }}

SELECT  t.AssetKey                                                       AS [Asset key]
  , NULLIF(t.AssetFunctionalLocationKey, '')                         AS [Asset functional location key]
  , NULLIF(t.AssetID, '')                                            AS [Asset ID]
  , NULLIF(t.Asset, 'AssetID') + '  (' + NULLIF(t.AssetID, '') + ')' AS [Asset]
  , COALESCE (t.Asset, 'AssetID', '')                                AS [Asset name]
  , NULLIF(t.AssetState, '')                                         AS [Asset state]
  , NULLIF(t.AssetType, '')                                          AS [Asset type]
  , NULLIF(t.FixedAssetID, '')                                       AS [Fixed asset ID]
  , NULLIF(t.ResourceID, '')                                         AS [Resource ID]
  , pr.ProductionResourceKey                                         AS [Production resource key]
  , NULLIF(t.Model, '')                                              AS [Model]
  , NULLIF(t.ModelYear, '')                                          AS [Model year]
  , NULLIF(t.AssetValidFrom, '1/1/1900')                             AS [Asset valid from]
  , NULLIF(t.AssetValidTo, '1/1/1900')                               AS [Asset valid to]
FROM {{ ref("asset") }}                   t 
LEFT JOIN {{ ref("productionresource") }} pr
  ON pr.ResourceID = t.ResourceID
AND pr.LegalEntityID = t.LegalEntityID;
