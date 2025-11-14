{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/workergroup/workergroup.py
-- Root method: Workergroup.workergroupdetail [WorkerGroupDetail]
-- external_table_name: WorkerGroupDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY JT.recid) AS WorkerGroupKey
         , JT.dataareaid                              AS LegalEntityID
         , JT.workergroupid                              AS WorkerGroupID
         , ISNULL(NULLIF(JT.name, ''), JT.workergroupid) AS WorkerGroup
         , JT.recid                                   AS _RecID
         , 1                                             AS _SourceID
         ,  CURRENT_TIMESTAMP                                                          AS  _CreatedDate
         ,  CURRENT_TIMESTAMP                                                          AS  _ModifiedDate


      FROM {{ ref('entassetworkergroup') }} JT
