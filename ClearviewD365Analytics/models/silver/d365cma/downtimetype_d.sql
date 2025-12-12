{{ config(materialized='table', tags=['silver'], alias='downtimetype') }}

-- Source file: cma/cma/layers/_base/_silver/downtimetype/downtimetype.py
-- Root method: Downtimetype.downtimetypedetail [DowntimeTypeDetail]
-- external_table_name: DowntimeTypeDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['rt.recid']) }} AS DowntimeTypeKey 
    ,rt.dataareaid         AS LegalEntityID
         , rt.kpiinclude           AS KPIInclude
         , rt.name                 AS DowntimeTypeID
         , rt.productionstoptypeid AS DowntimeTypeName
         , rt.recid              AS _RecID
         , 1                       AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('entassetproductionstoptype') }} rt;

