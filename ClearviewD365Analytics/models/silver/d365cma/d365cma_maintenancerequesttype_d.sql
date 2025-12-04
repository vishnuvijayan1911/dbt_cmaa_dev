{{ config(materialized='table', tags=['silver'], alias='maintenancerequesttype') }}

-- Source file: cma/cma/layers/_base/_silver/maintenancerequesttype/maintenancerequesttype.py
-- Root method: Maintenancerequesttype.maintenancerequesttypedetail [MaintenanceRequestTypeDetail]
-- external_table_name: MaintenanceRequestTypeDetail
-- schema_name: temp

SELECT  
          {{ dbt_utils.generate_surrogate_key(['rty.recid']) }} AS MaintenanceRequestTypeKey
          ,rty.requesttypeid AS MaintenanceRequestTypeID

         , rty.dataareaid   AS LegalEntityID

         , rty.name          AS MaintenanceRequestType

         , rty.recid        AS _RecID

         , 1                 AS _SourceID



        ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM {{ ref('entassetrequesttype') }} rty

