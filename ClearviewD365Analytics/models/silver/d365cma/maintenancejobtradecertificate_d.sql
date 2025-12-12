{{ config(materialized='table', tags=['silver'], alias='maintenancejobtradecertificate') }}

-- Source file: cma/cma/layers/_base/_silver/maintenancejobtradecertificate/maintenancejobtradecertificate.py
-- Root method: Maintenancejobtradecertificate.maintenancejobtradecertificatedetail [MaintenanceJobTradeCertificateDetail]
-- external_table_name: MaintenanceJobTradeCertificateDetail
-- schema_name: temp

SELECT  {{ dbt_utils.generate_surrogate_key(['eajtc.recid']) }} AS MaintenanceJobTradeCertificateKey
         , eajtc.dataareaid                                           AS LegalEntityID
         , eajt.jobtradeid                                            AS JobTradeID
         , ISNULL(NULLIF(eajt.description, ''), eajt.jobtradeid)      AS JobTrade
         , hct.certificatetypeid                                      AS CertificateTypeID
         , ISNULL(NULLIF(hct.description, ''), hct.certificatetypeid) AS CertificateType
         , eajtc.recid                                               AS _RecID
         , 1                                                          AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('entassetjobtradecertificate') }} eajtc


      LEFT JOIN {{ ref('hcmcertificatetype') }}     hct
        ON hct.recid   = eajtc.hcmcertificatetype
      LEFT JOIN {{ ref('entassetjobtrade') }}       eajt
        ON eajt.recid  = eajtc.jobtrade;

