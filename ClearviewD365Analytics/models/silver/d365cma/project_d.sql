{{ config(materialized='table', tags=['silver'], alias='project') }}

-- Source file: cma/cma/layers/_base/_silver/project/project.py
-- Root method: Project.projectdetail [ProjectDetail]
-- Inlined methods: Project.projectstage [Projectstage]
-- external_table_name: ProjectDetail
-- schema_name: temp

WITH
projectstage AS (
    SELECT pt.dataareaid         AS LegalEntityID
             , pt.projid              AS ProjectID
             , pt.name                AS ProjectName
             , pt.status              AS ProjectStateID
             , pt.parentid            AS ParentProjectID
             , pt.projgroupid         AS ProjectGroupID
             , pg.name                AS ProjectGroup
             , pt.jobid               AS JobID
             , pt.custaccount         AS CustAccount
             , pt.recid              AS _RecID
             , 1                      AS _SourceID
             , NULLIF(pt1.projid, '') AS ProjectIDLevel1
             , NULLIF(pt1.name, '')   AS ProjectNameLevel1
             , NULLIF(pt2.projid, '') AS ProjectIDLevel2
             , NULLIF(pt2.name, '')   AS ProjectNameLevel2
             , NULLIF(pt3.projid, '') AS ProjectIDLevel3
             , NULLIF(pt3.name, '')   AS ProjectNameLevel3
             , NULLIF(pt4.projid, '') AS ProjectIDLevel4
             , NULLIF(pt4.name, '')   AS ProjectNameLevel4
             , NULLIF(pt5.projid, '') AS ProjectIDLevel5
             , NULLIF(pt5.name, '')   AS ProjectNameLevel5
             , NULLIF(pt6.projid, '') AS ProjectIDLevel6
             , NULLIF(pt6.name, '')   AS ProjectNameLevel6
             , NULLIF(pt7.projid, '') AS ProjectIDLevel7
             , NULLIF(pt7.name, '')   AS ProjectNameLevel7
             , NULLIF(pt8.projid, '') AS ProjectIDLevel8
             , NULLIF(pt8.name, '')   AS ProjectNameLevel8

          FROM {{ ref('projtable') }}      AS pt
          LEFT JOIN {{ ref('projgroup') }} pg
            ON pg.projgroupid  = pt.projgroupid
           AND pg.dataareaid  = pt.dataareaid
          LEFT JOIN {{ ref('projtable') }} pt1
            ON pt1.projid      = pt.parentid
           AND pt1.dataareaid = pt.dataareaid
          LEFT JOIN {{ ref('projtable') }} pt2
            ON pt2.projid      = pt1.parentid
           AND pt2.dataareaid = pt1.dataareaid
          LEFT JOIN {{ ref('projtable') }} pt3
            ON pt3.projid      = pt2.parentid
           AND pt3.dataareaid = pt2.dataareaid
          LEFT JOIN {{ ref('projtable') }} pt4
            ON pt4.projid      = pt3.parentid
           AND pt4.dataareaid = pt3.dataareaid
          LEFT JOIN {{ ref('projtable') }} pt5
            ON pt5.projid      = pt4.parentid
           AND pt5.dataareaid = pt4.dataareaid
          LEFT JOIN {{ ref('projtable') }} pt6
            ON pt6.projid      = pt5.parentid
           AND pt6.dataareaid = pt5.dataareaid
          LEFT JOIN {{ ref('projtable') }} pt7
            ON pt7.projid      = pt6.parentid
           AND pt7.dataareaid = pt6.dataareaid
          LEFT JOIN {{ ref('projtable') }} pt8
            ON pt8.projid      = pt7.parentid
           AND pt8.dataareaid = pt7.dataareaid;
)
SELECT {{ dbt_utils.generate_surrogate_key(['ts._RecID']) }} AS ProjectKey
         , ts.ProjectID
         , ts.ParentProjectID
         , ts.ProjectName
         , ts.LegalEntityID
         , ts.ProjectStateID
         , we.enumvalue     AS ProjectState
         , ts.ProjectGroupID
         , ts.ProjectGroup
         , ProjectIDLevel1
         , ProjectNameLevel1
         , ProjectIDLevel2
         , ProjectNameLevel2
         , ProjectIDLevel3
         , ProjectNameLevel3
         , ProjectIDLevel4
         , ProjectNameLevel4
         , ProjectIDLevel5
         , ProjectNameLevel5
         , ProjectIDLevel6
         , ProjectNameLevel6
         , ProjectIDLevel7
         , ProjectNameLevel7
         , ProjectIDLevel8
         , ProjectNameLevel8
         , ts._RecID
         , 1                AS _SourceID

         ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))    AS  _CreatedDate
         ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))    AS  _ModifiedDate
      FROM projectstage               ts
      LEFT JOIN {{ ref('enumeration') }} we
        ON we.enumvalueid = ts.ProjectStateID
       AND we.enum        = 'ProjStatus';

