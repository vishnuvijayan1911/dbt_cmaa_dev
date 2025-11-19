{{ config(materialized='table', tags=['silver'], alias='workordertype') }}

-- Source file: cma/cma/layers/_base/_silver/workordertype/workordertype.py
-- Root method: Workordertype.workordertypedetail [WorkOrderTypeDetail]
-- external_table_name: WorkOrderTypeDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY JT.recid) AS WorkOrderTypeKey
          ,JT.dataareaid                                   AS LegalEntityID
         , JT.workordertypeid                              AS WorkOrderTypeID
         , ISNULL(NULLIF(JT.name, ''), JT.workordertypeid) AS WorkOrderType
         , JT.recid                                        AS _RecID
         , 1                                               AS _SourceID

         ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))                                 AS  _CreatedDate
         ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))                                 AS  _ModifiedDate
      FROM {{ ref('entassetworkordertype') }} JT

