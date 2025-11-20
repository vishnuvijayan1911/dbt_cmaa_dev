{{ config(materialized='table', tags=['silver'], alias='logisticspostaladdress') }}

-- Source file: cma/cma/layers/_base/_silver/logisticspostaladdress/logisticspostaladdress.py
-- Root method: Logisticspostaladdress.logisticspostaladdressdetail [LogisticsPostalAddressDetail]
-- Inlined methods: Logisticspostaladdress.logisticspostaladdresscountry [LogisticsPostalAddressCountry]
-- external_table_name: LogisticsPostalAddressDetail
-- schema_name: temp

WITH
logisticspostaladdresscountry AS (
    SELECT *

          FROM (   SELECT lac.countryregionid                       AS CountryRegionID
                        , lac.isocode                                      AS ISOCode
                        , CAST(ISNULL(lt.shortname, ' ') AS VARCHAR(250)) AS CountryShortName
                        , CAST(ISNULL(lt.longname, ' ') AS VARCHAR(250))  AS CountryLongName
                        , ROW_NUMBER() OVER (PARTITION BY lac.countryregionid
    ORDER BY lac.countryregionid)                                          AS RankVal
                     FROM {{ ref('logisticsaddresscountryregion') }}            lac
                     JOIN {{ ref('logisticsaddresscountryregiontranslation') }} lt
                       ON lt.partition       = lac.partition
                      AND lt.countryregionid = lac.countryregionid
                    WHERE lt.languageid = 'en-us') t
         WHERE t.RankVal = 1;
)
SELECT *

      FROM (   SELECT lpa.recid                                             AS recid
                    , lpa.location                                          AS Location
                    , lpa.street                                            AS Street
                    , lpa.city                                              AS City
                    , lpa.state                                             AS State
                    , lpa.zipcode                                           AS ZipCode
                    , lpa.county                                            AS County
                    , tc.CountryRegionID                                    AS CountryRegionID
                    , ISNULL(tc.CountryShortName, '')                       AS Country
                    , lpa.districtname                                      AS DistrictName
                    , CAST(ISNULL(lpa.validfrom, '1900-01-01') AS DATE)     AS ValidFrom
                    , CAST(ISNULL(lpa.validto, '2154-12-31') AS DATE)       AS ValidTo
                    , CAST(ROW_NUMBER() OVER (PARTITION BY lpa.location
ORDER BY ISNULL(lpa.validto, CAST('2154-12-31' AS DATE)) DESC) AS SMALLINT) AS LocationRank
                    ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))                                       AS  _CreatedDate
                    ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))                                       AS  _ModifiedDate
                 FROM {{ ref('logisticspostaladdress') }} lpa
                 LEFT JOIN logisticspostaladdresscountry                        tc
                   ON tc.CountryRegionID = lpa.countryregionid) t
     WHERE t.LocationRank = 1;

