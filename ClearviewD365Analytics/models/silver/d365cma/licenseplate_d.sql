{{ config(materialized='table', tags=['silver'], alias='licenseplate') }}

-- Source file: cma/cma/layers/_base/_silver/licenseplate/licenseplate.py
-- Root method: Licenseplate.licenseplatedetail [LicensePlateDetail]
-- external_table_name: LicensePlateDetail
-- schema_name: temp

SELECT *, {{ dbt_utils.generate_surrogate_key(['t.LegalEntityID', 't.LicensePlate']) }} AS LicensePlateKey
   ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
         FROM (
        SELECT DISTINCT 
          id.dataareaid         AS LegalEntityID
         , id.licenseplateid     AS LicensePlate
         , wlp.containertypecode AS ContainerType
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('inventdim') }}            id
      LEFT JOIN {{ ref('whslicenseplate') }} wlp
        ON wlp.dataareaid     = id.dataareaid
       AND wlp.licenseplateid = id.licenseplateid) t

