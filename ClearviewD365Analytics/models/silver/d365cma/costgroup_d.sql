{{ config(materialized='table', tags=['silver'], alias='costgroup') }}

-- Source file: cma/cma/layers/_base/_silver/costgroup/costgroup.py
-- Root method: Costgroup.costgroupdetail [CostGroupDetail]
-- Inlined methods: Costgroup.costgroupstage [CostGroupStage], Costgroup.costgroupdetail1 [CostGroupDetail1], Costgroup.costgroupdetail2 [CostGroupDetail2]
-- external_table_name: CostGroupDetail
-- schema_name: temp

WITH
costgroupstage AS (
    SELECT DISTINCT
               CASE WHEN cg.costgrouptype = 2 AND cg.costgroupid <> 'DL' THEN 'DOH' ELSE cg.costgroupid END           AS CostGroupID
             , MAX(CASE WHEN cg.costgrouptype = 2 AND cg.costgroupid <> 'DL' THEN 'Direct Overhead' ELSE cg.name END) AS CostGroup
             , MAX(cg.costgrouptype)                                                                                  AS CostGroupType

          FROM {{ ref('bomcostgroup') }} cg
         GROUP BY cg.costgroupid
                , cg.costgrouptype;
),
costgroupdetail1 AS (
    SELECT ROW_NUMBER() OVER (ORDER BY ts.CostGroupID) AS CostGroupKey
             , ts.CostGroupID   AS CostGroupID
             , ts.CostGroup     AS CostGroup
             , ts.CostGroupType AS CostGroupTypeID
             , we.enumvalue     AS CostGroupType
             , cb.CostBucketID  AS CostBucketID

          FROM costgroupstage               ts
         LEFT JOIN {{ ref('costgroup') }}   ucg
            ON ucg.costgroupid = ts.CostGroupID
          LEFT JOIN {{ ref('enumeration') }} we
            ON we.enum         = 'CostGroupType'
           AND we.enumvalueid  = ts.CostGroupType
          LEFT JOIN silver.cma_CostBucket  cb
            ON cb.CostBucketID = ISNULL(ucg.costbucketid, 'MAT');
),
costgroupdetail2 AS (
    SELECT ROW_NUMBER() OVER (ORDER BY ucg.costgroupid) AS CostGroupKey
              ,ucg.costgroupid AS CostGroupID
             , cb.CostBucket   AS CostGroup
             , 0               AS CostGroupTypeID
             , 'Undefined'     AS CostGroupType
             , cb.CostBucketID AS CostBucketID

          FROM {{ ref('costgroup') }}  ucg
          JOIN silver.cma_CostBucket cb
            ON cb.CostBucketID = ucg.costbucketid
         WHERE NOT EXISTS (SELECT 1 FROM costgroupdetail1 d1 WHERE d1.CostGroupID = ucg.costgroupid);
)
SELECT *

        ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM (SELECT * FROM costgroupdetail1 UNION SELECT * FROM costgroupdetail2) t;

