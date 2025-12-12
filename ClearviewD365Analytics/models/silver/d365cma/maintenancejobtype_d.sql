{{ config(materialized='table', tags=['silver'], alias='maintenancejobtype') }}

-- Source file: cma/cma/layers/_base/_silver/maintenancejobtype/maintenancejobtype.py
-- Root method: Maintenancejobtype.maintenancejobtypedetail [MaintenanceJobTypeDetail]
-- external_table_name: MaintenanceJobTypeDetail
-- schema_name: temp

SELECT
          {{ dbt_utils.generate_surrogate_key(['jt.recid']) }} AS MaintenanceJobTypeKey 
          ,jt.dataareaid                                   AS LegalEntityID

         , jt.jobtypeid                                     AS MaintenanceJobTypeID

         , ISNULL(NULLIF(jt.description, ''), jt.jobtypeid) AS MaintenanceJobType

         , jt.recid                                        AS _RecID

         , 1                                                AS _SourceID



         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('entassetjobtype') }} jt

