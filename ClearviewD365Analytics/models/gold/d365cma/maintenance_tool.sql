{{ config(materialized='view', schema='gold', alias="Maintenance tool") }}

SELECT  t.ProductionResourceKey       AS [Resource key]
  , NULLIF(t.ResourceID, '')      AS [Tool]
  , NULLIF(t.Resource, '')        AS [Tool name]
  , NULLIF(t.ResourceGroupID, '') AS [Tool group]
  , NULLIF(t.ResourceGroup, '')   AS [Tool group name]
FROM {{ ref("ProductionResource") }} t
WHERE EXISTS (SELECT  1 FROM {{ ref("WorkOrderLineTool_Fact") }} f WHERE f.ResourceKey = t.ProductionResourceKey)
