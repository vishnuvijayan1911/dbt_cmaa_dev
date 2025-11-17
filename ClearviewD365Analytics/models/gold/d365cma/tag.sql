{{ config(materialized='view', schema='gold', alias="Tag") }}

SELECT  t.TagKey                                                       AS [Tag key]
      , ISNULL(dv.VendorKey, -1)                                        AS [Vendor key]
      , ISNULL(dd.DateKey, -1)                                          AS [Manufacturing date key]
      , CASE WHEN t.TagKey <> -1 THEN CAST(1 AS SMALLINT)ELSE NULL END  AS [Tags]
      , NULLIF(t.TagID, '')                                             AS [Tag #]
      , NULLIF(t.ItemID, '')                                           AS [Item #]
      , NULLIF(t.HeatNumber, '')                                       AS [Heat #]
      , NULLIF(t.OriginCountryID, '')                                  AS [Origin country]
      , NULLIF(t.OriginCountry, '')                                    AS [Origin country name]
      , NULLIF(t.DispositionCode, '')                                  AS [Disposition code]
      , NULLIF(t.DispositionStatus, '')                                AS [Disposition status]
      , NULLIF(t.OutsideProcessorTag, '')                              AS [Outside processor tag]
      , NULLIF(t.MasterTagID, '')                                      AS [Master tag #]
      , NULLIF(t.MillTagID, '')                                        AS [Mill tag #]
      , NULLIF(t.TagType, '')                                          AS [Tag type]
      , NULLIF (t.Silver, '') AS [Silver],
NULLIF (t.Aluminum, '') AS [Aluminum],
NULLIF (t.Boron, '') AS [Boron],
NULLIF (t.Beryllium, '') AS [Beryllium],
TRY_CONVERT (NUMERIC(20, 8), t.CoilWeight)   AS [Coil weight],
TRY_CONVERT (NUMERIC(20, 8), t.InnerDiameter)   AS [Inner diameter],
TRY_CONVERT (NUMERIC(20, 8), t.Length)   AS [Length],
TRY_CONVERT (NUMERIC(20, 8), t.OuterDiameter)   AS [Outer diameter],
NULLIF (t.PIW, '') AS [PIW],
NULLIF (t.Quality, '') AS [Quality],
TRY_CONVERT (NUMERIC(20, 8), t.Thickness)   AS [Thickness],
TRY_CONVERT (NUMERIC(20, 8), t.Width)   AS [Width],
TRY_CONVERT (NUMERIC(20, 8), t.Yield)   AS [Yield],
NULLIF (t._1E0166, '') AS [1E0166],
NULLIF (t._1E4664, '') AS [1E4664],
NULLIF (t._1E0065, '') AS [1E0065],
NULLIF (t._1E1863, '') AS [1E1863],
NULLIF (t._1E1006, '') AS [1E1006],
NULLIF (t._1E577, '') AS [1E577],
NULLIF (t._1E1839, '') AS [1E1839],
NULLIF (t._1E860, '') AS [1E860],
NULLIF (t._1E357, '') AS [1E357],
NULLIF (t._1E1247, '') AS [1E1247],
NULLIF (t._1E1883, '') AS [1E1883],
NULLIF (t._1E0170, '') AS [1E0170]
      , NULLIF(t.VendorAccount, '')                                    AS [Vendor account]
      , NULLIF(t.VendorTagID, '')                                      AS [Vendor tag #]
      , NULLIF(t.BestBeforeDate, '1/1/1900')                           AS [Best before date]
      , NULLIF(t.ExpirationDate, '1/1/1900')                           AS [Expiration date]
      , NULLIF(t.ProductionDate, '1/1/1900')                           AS [Production date]
    FROM {{ ref("TagAttribute") }}         t
    LEFT JOIN {{ ref('date') }}   dd
      ON dd.Date          = t.ProductionDate
    LEFT JOIN {{ ref("Vendor") }} dv
      ON dv.LegalEntityID = t.LegalEntityID
    AND dv.VendorAccount = t.VendorAccount;
