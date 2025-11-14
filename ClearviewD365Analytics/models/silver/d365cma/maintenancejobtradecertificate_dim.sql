{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/maintenancejobtradecertificate/maintenancejobtradecertificate.py
-- Root method: Maintenancejobtradecertificate.maintenancejobtradecertificatedetail [MaintenanceJobTradeCertificateDetail]
-- external_table_name: MaintenanceJobTradeCertificateDetail
-- schema_name: temp

SELECT  ROW_NUMBER() OVER (ORDER BY eajtc.recid) AS MaintenanceJobTradeCertificateKey
         , eajtc.dataareaid                                           AS LegalEntityID
         , eajt.jobtradeid                                            AS JobTradeID
         , ISNULL(NULLIF(eajt.description, ''), eajt.jobtradeid)      AS JobTrade
         , hct.certificatetypeid                                      AS CertificateTypeID
         , ISNULL(NULLIF(hct.description, ''), hct.certificatetypeid) AS CertificateType
         , eajtc.recid                                               AS _RecID
         , 1                                                          AS _SourceID

      FROM {{ ref('entassetjobtradecertificate') }} eajtc


      LEFT JOIN {{ ref('hcmcertificatetype') }}     hct
        ON hct.recid   = eajtc.hcmcertificatetype
      LEFT JOIN {{ ref('entassetjobtrade') }}       eajt
        ON eajt.recid  = eajtc.jobtrade;
