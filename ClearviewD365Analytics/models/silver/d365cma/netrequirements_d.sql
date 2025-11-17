{{ config(materialized='table', tags=['silver'], alias='netrequirements') }}

-- Source file: cma/cma/layers/_base/_silver/netrequirements/netrequirements.py
-- Root method: NetRequirements.netrequirementsdetail [NetRequirementsDetail]
-- external_table_name: NetRequirementsDetail
-- schema_name: temp

SELECT ROW_NUMBER () OVER (ORDER BY t._RecID) AS NetRequirementsKey
     , t.*
  FROM (   SELECT rt.dataareaid                                                                                                  AS LegalEntityID
                , de.enumvalue                                                                                                   AS BomType
                , de1.enumvalue                                                                                                  AS Direction
                , CASE WHEN rt.intercompanyplannedorder = 0 THEN 'No' ELSE 'Yes' END                                             AS IsIntercompanyPlannedOrder
                , CASE WHEN rt.isderiveddirectly = 0 THEN 'No' ELSE 'Yes' END                                                    AS IsDirectlyDerived
                , CASE WHEN rt.isdelayed = 0 THEN 'No' ELSE 'Yes' END                                                            AS IsDelayed
                , rpv.reqplanid                                                                                                  AS PlanVersion
                , rt.refid                                                                                                       AS Reference
                , de2.enumvalue                                                                                                  AS ReferenceType
                , pt.name                                                                                                        AS ProductionName
                , de3.enumvalue                                                                                                  AS ProductionStatus
                , po.reqpostatus                                                                                                 AS ReqPOStatus
                , rt.covinventdimid                                                                                              AS InventDimID
                , pt.createdby                                                                                                   AS OrderCreatedBy
                , CAST(pt.createddatetime AS Datetime) AT TIME ZONE 'UTC' AT TIME ZONE le.TimeZone                               AS OrderCreatedDateTime
                , rt.recid                                                                                                       AS _RecID
                , 1                                                                                                              AS _SourceID
                , CURRENT_TIMESTAMP                                                                                              AS _CreatedDate
                , CURRENT_TIMESTAMP                                                                                              AS _ModifiedDate
             FROM {{ ref('reqtrans') }}            rt
             INNER JOIN {{ ref('legalentity_d') }}         le
			         ON le.LegalEntityID   = rt.dataareaid
             LEFT JOIN {{ ref('prodtable') }}      pt
               ON pt.dataareaid       = rt.dataareaid
              AND pt.collectrefprodid = rt.refid
              AND rt.reftype IN ( 9, 12 ) --Production or production line
             LEFT JOIN {{ ref('reqpo') }}          po
               ON rt.dataareaid       = po.dataareaid
              AND rt.refid            = po.refid
              AND rt.planversion      = po.planversion
             LEFT JOIN {{ ref('enumeration') }}    de
               ON de.enumvalueid      = rt.bomtype
              AND enum                = 'BOMType'
             LEFT JOIN {{ ref('enumeration') }}    de1
               ON de1.enumvalueid     = rt.direction
              AND de1.enum            = 'InventDirection'
             LEFT JOIN {{ ref('enumeration') }}    de2
               ON de2.enumvalueid     = rt.reftype
              AND de2.enum            = 'ReqRefType'
             LEFT JOIN {{ ref('enumeration') }}    de3
               ON de3.enumvalueid     = pt.prodstatus
              AND de3.enum            = 'ProdStatus'
             LEFT JOIN {{ ref('reqplanversion') }} rpv
               ON rpv.recid           = rt.planversion) t;

