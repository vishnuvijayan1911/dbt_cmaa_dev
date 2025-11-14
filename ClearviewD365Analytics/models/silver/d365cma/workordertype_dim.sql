{{ config(materialized='table', tags=['silver']) }}

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
         ,  CURRENT_TIMESTAMP                                 AS  _CreatedDate
         ,  CURRENT_TIMESTAMP                                 AS  _ModifiedDate

      FROM {{ ref('entassetworkordertype') }} JT
