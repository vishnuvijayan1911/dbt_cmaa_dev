{{ config(materialized='view', schema='gold', alias="Production BOM") }}

SELECT  t.ProductionBOMKey                                                               AS [Production BOM key]
  , CASE WHEN t.IsBOQA = 1 THEN 'BOQA' ELSE 'Not BOQA' END                           AS [BOQA]
  , CASE WHEN t.IsRecoverableScrap = 1 THEN 'Recoverable' ELSE 'Not Recoverable' END AS [Recoverable scrap]
  , CASE WHEN t.IsRTS = 1 THEN 'RTS' ELSE 'Not RTS' END                              AS [RTS]
  , CASE WHEN t.IsOSP = 1 THEN 'OSP' ELSE 'Not OSP' END                              AS [OSP]
  , NULLIF(t.LineNumber, '')                                                         AS [Line #]
  , NULLIF(t.OperationNumber, '')                                                    AS [Operation #]
  , NULLIF(t.ProductionID, '')                                                       AS [Production #]
  , NULLIF(t.ProductionUOM, '')                                                      AS [Production UOM]
  , NULLIF(t.ReferenceID, '')                                                        AS [Reference #]
  , NULLIF(t.TransReference, '')                                                     AS [Trans reference #]
FROM {{ ref("d365cma_productionbom_d") }} t ;
