{{ config(materialized='table', tags=['silver'], alias='downtimetype') }}

-- Source file: cma/cma/layers/_base/_silver/downtimetype/downtimetype.py
-- Root method: Downtimetype.downtimetypedetail [DowntimeTypeDetail]
-- external_table_name: DowntimeTypeDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY rt.recid) AS DowntimeTypeKey 
    ,rt.dataareaid         AS LegalEntityID
         , rt.kpiinclude           AS KPIInclude
         , rt.name                 AS DowntimeTypeID
         , rt.productionstoptypeid AS DowntimeTypeName
         , rt.recid              AS _RecID
         , 1                       AS _SourceID
         ,  CURRENT_TIMESTAMP    AS  _CreatedDate
         ,  CURRENT_TIMESTAMP    AS  _ModifiedDate

      FROM {{ ref('entassetproductionstoptype') }} rt;

