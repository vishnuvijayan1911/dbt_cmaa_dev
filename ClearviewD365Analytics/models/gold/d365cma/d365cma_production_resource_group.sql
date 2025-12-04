{{ config(materialized='view', schema='gold', alias="Production resource group") }}

SELECT DISTINCT
       CONCAT (t.LegalEntityID, t.ResourceGroupID) AS [Resource group index]
     , NULLIF (t.LegalEntityID, '')                AS [Legal entity]
     , NULLIF (t.ResourceGroupID, '')              AS [Resource group]
     , NULLIF (t.ResourceGroup, '')                AS [Resource group name]
  FROM {{ ref('d365cma_productionresource_d') }} t;
