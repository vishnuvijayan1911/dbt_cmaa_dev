{{ config(materialized='table', tags=['silver'], alias='tag') }}

-- Source file: cma/cma/layers/_base/_silver/tag/tag.py
-- Root method: Tag.tagdetail [TagDetail]
-- Inlined methods: Tag.tagcountry [TagCountry]
-- external_table_name: TagDetail
-- schema_name: temp

WITH
tagcountry AS (
    SELECT *

          FROM (   SELECT lac.countryregionid                       AS CountryRegionID
                        , CAST(ISNULL(lt.shortname, ' ') AS VARCHAR(250)) AS CountryShortName
                        , ROW_NUMBER() OVER (PARTITION BY lac.countryregionid
    ORDER BY lac.countryregionid)                                          AS RankVal
                     FROM {{ ref('logisticsaddresscountryregion') }}            lac
                     JOIN {{ ref('logisticsaddresscountryregiontranslation') }} lt
                       ON lt.partition       = lac.partition
                      AND lt.countryregionid = lac.countryregionid
                    WHERE lt.languageid = 'en-us') t
         WHERE t.RankVal = 1;
)
SELECT ROW_NUMBER() OVER (ORDER BY ib.recid) AS TagKey
          ,ib.dataareaid                                                                             AS LegalEntityID
         , ib.cmartsparent                                                                            AS ParentTagID
         , ib.inventbatchid                                                                           AS TagID
         , ib.itemid                                                                                  AS ItemID
         , ib.pdsbestbeforedate                                                                       AS BestBeforeDate

         , ib.pdsdispositioncode                                                                      AS DispositionCode
         , we.enumvalue                                                                               AS DispositionStatus
         , ib.expdate                                                                                 AS ExpirationDate
         , ib.cmaheatnumber                                                                           AS HeatNumber
         , ib.cmamasterinventbatch                                                                    AS MasterTagID
         , ib.cmamilltagnumber                                                                        AS MillTagID
         , ib.cmacountryoforigin                                                                      AS OriginCountryID
         , CASE WHEN tc.CountryShortName = '' THEN ib.cmacountryoforigin ELSE tc.CountryShortName END AS OriginCountry
         , ib.cmaoutsideprocesstag                                                                    AS OutsideProcessorTag
         , ib.proddate                                                                                AS ProductionDate
         , ib.pdsvendbatchid                                                                          AS VendorTagID
         , CASE WHEN ib.inventbatchid = ib.cmamasterinventbatch THEN 'Master tag' ELSE 'Remnant' END  AS TagType
         , ib.cmavendaccount                                                                          AS VendorAccount
         , ib.recid                                                                                   AS _RecID
         , 1                                                                                          AS _SourceID
         ,'1900-01-01'                                                                                AS ActivityDate   
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                          AS _ModifiedDate
      FROM {{ ref('inventbatch') }}  ib
      LEFT JOIN tagcountry   tc
        ON tc.CountryRegionID = ib.cmacountryoforigin
      LEFT JOIN {{ ref('pdsdispositionmaster') }} pdm
	    on pdm.dispositioncode = ib.pdsdispositioncode
        and pdm.dataareaid = ib.dataareaid
      LEFT JOIN {{ ref('enumeration') }}           we
	    on we.enum            = 'PdsStatus'
	   AND we.enumvalueid     =  pdm.status
     WHERE ib.inventbatchid <> '0'
       AND ib.inventbatchid <> ''

