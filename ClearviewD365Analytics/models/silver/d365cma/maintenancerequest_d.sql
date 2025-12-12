{{ config(materialized='table', tags=['silver'], alias='maintenancerequest') }}

-- Source file: cma/cma/layers/_base/_silver/maintenancerequest/maintenancerequest.py
-- Root method: Maintenancerequest.maintenancerequestdetail [MaintenanceRequestDetail]
-- external_table_name: MaintenanceRequestDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['rt.recid']) }} AS MaintenanceRequestKey
        , CAST(rt.actualstart AS DATE) AS ActualStartDate
         , CAST(rt.actualend AS DATE)   AS ActualEndDate
         , rt.description               AS RequestDesc

         , rt.requestid                 AS MaintenanceRequestID
         , rt.dataareaid               AS LegalEntityID
         , CAST(slog.created AS DATE)   AS RequestCreateDate
         , slog.createdby              AS RequestCreateBy
         , rt.recid                    AS _RecID
         , 1                            AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('entassetrequesttable') }}           rt
      LEFT JOIN {{ ref('entassetlifecyclestatelog') }} slog
        ON rt.dataareaid = slog.dataareaid
       AND rt.recid      = slog.refrecid
       AND slog.remark IN ( 'Request Created', 'Maintenance request created', 'Anmodning Oprettet' );

