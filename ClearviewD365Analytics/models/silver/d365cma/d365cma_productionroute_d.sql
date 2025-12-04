{{ config(materialized='table', tags=['silver'], alias='productionroute') }}

-- Source file: cma/cma/layers/_base/_silver/productionroute/productionroute.py
-- Root method: Productionroute.productionroutedetail [ProductionRouteDetail]
-- Inlined methods: Productionroute.productionroutestage [ProductionRouteStage]
-- external_table_name: ProductionRouteDetail
-- schema_name: temp

WITH
productionroutestage AS (
    SELECT pr.dataareaid                                                AS LegalEntityID
             , pr.oprfinished                                                AS OperationFinished
             , pr.oprnum                                                     AS OperationNumber
             , pr.oprpriority                                                AS OprPriority
             , pr.jobidprocess                                               AS ProcessJobID
             , pr.qtycategoryid                                              AS QuantityCostCategoryID
             , pr.qtycategoryid                                              AS QuantityCostCategory
             , pr.routegroupid                                               AS RouteGroupID
             , CASE WHEN rg.name = '' THEN pr.routegroupid ELSE rg.name END  AS RouteGroup
             , pr.processcategoryid                                          AS RunCostCategoryID
             , pr.processcategoryid                                          AS RunCostCategory
             , CAST(CAST(pr.fromtime / 3600 AS VARCHAR(2)) + ':' + CAST((pr.fromtime % 3600) / 60 AS VARCHAR(2)) + ':'
                    + CAST((pr.fromtime % 3600) % 60 AS VARCHAR(2)) AS TIME) AS ScheduleStartTime
             , CAST(CAST(pr.totime / 3600 AS VARCHAR(2)) + ':' + CAST((pr.totime % 3600) / 60 AS VARCHAR(2)) + ':'
                    + CAST((pr.totime % 3600) % 60 AS VARCHAR(2)) AS TIME)   AS ScheduleEndTime
             , pr.setupcategoryid                                            AS SetupCostCategoryID
             , pr.setupcategoryid                                            AS SetupCostCategory
             , pr.cmasubcontractitem                                         AS SubContractItem
             , pr.cmasubcontractingunit                                      AS SubContractUOM
             , CASE WHEN pr.routetype = 1 THEN 1 ELSE 0 END                  AS IsOSP
             , 1                                                             AS _SourceID
             , pr.recid                                                      AS _RecID

          FROM {{ ref('prodroute') }}       pr
          LEFT JOIN {{ ref('routegroup') }} rg
            ON rg.dataareaid   = pr.dataareaid
           AND rg.routegroupid = pr.routegroupid;
)
SELECT 
          {{ dbt_utils.generate_surrogate_key(['ts._RecID', 'ts._SourceID']) }} AS ProductionRouteKey
         , ts.LegalEntityID                                                           AS LegalEntityID
         , CASE WHEN ts.OperationFinished = 1 THEN 'Finished' ELSE 'Not finished' END AS OperationFinished
         , ts.OperationNumber                                                         AS OperationNumber
         , ts.ProcessJobID                                                            AS ProcessJobID
         , ts.QuantityCostCategoryID                                                  AS QuantityCostCategoryID
         , ts.QuantityCostCategory                                                    AS QuantityCostCategory
         , ts.OprPriority                                                             AS RequirementTypeID
         , e1.enumvalue                                                               AS RequirementType
         , ts.RunCostCategoryID                                                       AS RunCostCategoryID
         , ts.RunCostCategory                                                         AS RunCostCategory
         , ts.ScheduleStartTime                                                       AS ScheduleStartTime
         , ts.ScheduleEndTime                                                         AS ScheduleEndTime
         , ts.SetupCostCategoryID                                                     AS SetupCostCategoryID
         , ts.SetupCostCategory                                                       AS SetupCostCategory
         , ts.SubContractItem                                                         AS SubContractItem
         , ts.SubContractUOM                                                          AS SubContractUOM
         , ts.IsOSP                                                                   AS IsOSP
         , ts._SourceID                                                               AS _SourceID
         , ts._RecID                                                                  AS _RecID

         ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM productionroutestage               ts
      LEFT JOIN {{ ref('enumeration') }} e1
        ON e1.enum        = 'RouteOprPriority'
       AND e1.enumvalueid = ts.OprPriority;

