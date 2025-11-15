{{ config(materialized='table', tags=['silver'], alias='licenseplate_dim') }}

-- Source file: cma/cma/layers/_base/_silver/licenseplate/licenseplate.py
-- Root method: Licenseplate.licenseplatedetail [LicensePlateDetail]
-- external_table_name: LicensePlateDetail
-- schema_name: temp

SELECT *, ROW_NUMBER() OVER (ORDER BY t.LegalEntityID, t.LicensePlate) AS LicensePlateKey
   ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate
         FROM (
        SELECT DISTINCT 
          id.dataareaid         AS LegalEntityID
         , id.licenseplateid     AS LicensePlate
         , wlp.containertypecode AS ContainerType
      FROM {{ ref('inventdim') }}            id
      LEFT JOIN {{ ref('whslicenseplate') }} wlp
        ON wlp.dataareaid     = id.dataareaid
       AND wlp.licenseplateid = id.licenseplateid) t
