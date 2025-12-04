{{ config(materialized='view', schema='gold', alias="Production resource") }}

SELECT t.ProductionResourceKey                     AS [Production resource key]
     , CONCAT (t.LegalEntityID, t.ResourceGroupID) AS [Resource group index]
     , NULLIF (t.LegalEntityID, '')                AS [Legal entity]
     , NULLIF (t.ResourceID, '')                   AS [Resource]
     , NULLIF (t.Resource, '')                     AS [Resource name]
     , CASE WHEN t.ResourceID IN ( 'DL20', 'DL8', 'KR', 'SG' )
            THEN 'Draw'
            WHEN t.ResourceID IN ( 'NCFNE', 'NCFLMP', 'THERMCO', 'TUFTS GR', 'SUPERSTEEL', 'BARPROCESSING' )
            THEN 'OSP'
            WHEN t.ResourceID IN ( 'CH-I', 'CH-II', 'CSAW', 'LGSH', 'OSTR', 'OTST', 'RETG' )
            THEN 'Non-draw'
            ELSE 'Other' END                       AS [Resource process type]
     , NULLIF (t.ResourceGroupID, '')              AS [Resource group]
     , NULLIF (t.ResourceGroup, '')                AS [Resource group name]
     , NULLIF (t.ResourceType, '')                 AS [Resource type]
  FROM {{ ref('d365cma_productionresource_d') }} t;
