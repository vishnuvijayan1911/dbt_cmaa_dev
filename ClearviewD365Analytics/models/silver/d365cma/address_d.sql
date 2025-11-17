{{ config(materialized='table', tags=['silver'], alias='address') }}

-- Source file: cma/cma/layers/_base/_silver/address/address.py
-- Root method: Address.addressdetail [AddressDetail]
-- Inlined methods: Address.addresscountry [AddressCountry]
-- external_table_name: AddressDetail
-- schema_name: temp

WITH
addresscountry AS (
    SELECT *
      FROM (   SELECT lac.countryregionid                       AS CountryRegionID
                        , lac.isocode                                      AS ISOCode
                        , CAST(ISNULL(lt.shortname, ' ') AS VARCHAR(250)) AS CountryShortName
                        , CAST(ISNULL(lt.longname, ' ') AS VARCHAR(250))  AS CountryLongName
                        , ROW_NUMBER() OVER (PARTITION BY lac.countryregionid
    ORDER BY lac.countryregionid)                                          AS RankVal
        FROM {{ ref('logisticspostaladdress') }}                        lpa
          INNER JOIN {{ ref('logisticsaddresscountryregion') }}            lac
          ON lac.countryregionid = lpa.countryregionid
          INNER JOIN {{ ref('logisticsaddresscountryregiontranslation') }} lt
          ON lt.partition        = lac.partition
            AND lt.countryregionid  = lac.countryregionid
        WHERE lt.languageid = 'en-us' ) t
      WHERE t.RankVal = 1
)
SELECT
    ROW_NUMBER() OVER (ORDER BY t._RecID) AS AddressKey,
    *
        , CURRENT_TIMESTAMP                                          AS _CreatedDate
        , CURRENT_TIMESTAMP                                         AS _ModifiedDate
        , '1900-01-01'                                               AS ActivityDate
  FROM (   SELECT lpa.location                                                                            AS Location
                    , lpa.street                                                                              AS Street
                    , lpa.city                                                                                AS City
                    , lpa.state                                                                               AS StateProvince
                    , lpa.zipcode                                                                             AS PostalCode
                    , tc.CountryRegionID                                                                      AS CountryID
                    , CASE WHEN tc.CountryShortName = '' THEN tc.CountryRegionID ELSE tc.CountryShortName END AS Country
                    , CAST(ROW_NUMBER() OVER (PARTITION BY lpa.location
ORDER BY ISNULL(lpa.validto, CAST('2154-12-31' AS DATE)) DESC) AS SMALLINT)                                   AS LocationRank
                    , lpa.recid                                                                               AS _RecID
                    , 1                                                                                       AS _SourceID
    FROM {{ ref('logisticspostaladdress') }} lpa
      LEFT JOIN addresscountry             tc
      ON tc.CountryRegionID = lpa.countryregionid ) t;
