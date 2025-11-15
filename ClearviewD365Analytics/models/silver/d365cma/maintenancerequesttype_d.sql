{{ config(materialized='table', tags=['silver'], alias='maintenancerequesttype_dim') }}

-- Source file: cma/cma/layers/_base/_silver/maintenancerequesttype/maintenancerequesttype.py
-- Root method: Maintenancerequesttype.maintenancerequesttypedetail [MaintenanceRequestTypeDetail]
-- external_table_name: MaintenanceRequestTypeDetail
-- schema_name: temp

SELECT  
          ROW_NUMBER() OVER (ORDER BY rty.recid) AS MaintenanceRequestTypeKey
          ,rty.requesttypeid AS MaintenanceRequestTypeID

         , rty.dataareaid   AS LegalEntityID

         , rty.name          AS MaintenanceRequestType

         , rty.recid        AS _RecID

         , 1                 AS _SourceID

        ,CURRENT_TIMESTAMP                                               AS _CreatedDate

        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM {{ ref('entassetrequesttype') }} rty
