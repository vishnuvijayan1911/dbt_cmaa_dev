{{ config(materialized='table', tags=['silver'], alias='workordercriticality') }}

-- Source file: cma/cma/layers/_base/_silver/workordercriticality/workordercriticality.py
-- Root method: Workordercriticality.workordercriticalitydetail [WorkOrderCriticalityDetail]
-- external_table_name: WorkOrderCriticalityDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY JT.recid) AS WorkOrderCriticalityKey
          ,JT.dataareaid  AS LegalEntityID
         , JT.criticality  AS WorkOrderCriticalityID
         , JT.name         AS WorkOrderCriticality
         , JT.ratingfactor AS CriticalityRatingFactor
         , JT.recid       AS _RecID
         , 1               AS _SourceID

         ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))    AS  _CreatedDate
         ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))    AS  _ModifiedDate
      FROM {{ ref('entassetcriticality') }} JT

