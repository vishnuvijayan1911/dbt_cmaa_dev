{{ config(materialized='table', tags=['silver'], alias='workergroup') }}

-- Source file: cma/cma/layers/_base/_silver/workergroup/workergroup.py
-- Root method: Workergroup.workergroupdetail [WorkerGroupDetail]
-- external_table_name: WorkerGroupDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['JT.recid']) }} AS WorkerGroupKey
         , JT.dataareaid                              AS LegalEntityID
         , JT.workergroupid                              AS WorkerGroupID
         , ISNULL(NULLIF(JT.name, ''), JT.workergroupid) AS WorkerGroup
         , JT.recid                                   AS _RecID
         , 1                                             AS _SourceID


         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('entassetworkergroup') }} JT

