{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/maintenancejobtype/maintenancejobtype.py
-- Root method: Maintenancejobtype.maintenancejobtypedetail [MaintenanceJobTypeDetail]
-- external_table_name: MaintenanceJobTypeDetail
-- schema_name: temp

SELECT
          ROW_NUMBER() OVER (ORDER BY jt.recid) AS MaintenanceJobTypeKey 
          ,jt.dataareaid                                   AS LegalEntityID

         , jt.jobtypeid                                     AS MaintenanceJobTypeID

         , ISNULL(NULLIF(jt.description, ''), jt.jobtypeid) AS MaintenanceJobType

         , jt.recid                                        AS _RecID

         , 1                                                AS _SourceID

        ,CURRENT_TIMESTAMP                                               AS _CreatedDate

        , CURRENT_TIMESTAMP                                               AS _ModifiedDate            

      FROM {{ ref('entassetjobtype') }} jt
