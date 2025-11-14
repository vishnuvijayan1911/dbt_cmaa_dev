{{ config(materialized='view', schema='gold', alias="Product") }}

SELECT  DISTINCT
        t.ProductKey                                                                                               AS [Product key]
       , NULLIF (le.LegalEntityID, '')                                                                              AS [Legal entity]
      , NULLIF (t.Commodity, '') AS [Commodity],
NULLIF (t.Condition, '') AS [Condition],
NULLIF (t.CustomerNumber, '') AS [Customer number],
NULLIF (t.FormShapeDimension1, '') AS [Form shape dimension1],
NULLIF (t.FormShapeDimension2, '') AS [Form shape dimension2],
NULLIF (t.FormShapeDimension3, '') AS [Form shape dimension3],
NULLIF (t.FormShape, '') AS [Form shape],
NULLIF (t.GradeAlloy, '') AS [Grade alloy],
NULLIF (t.PartNumber, '') AS [Part number],
NULLIF (t.RawMaterial, '') AS [Raw material],
NULLIF (t.RevisionNumber, '') AS [Revision number],
TRY_CONVERT (NUMERIC(20, 8), t.Gauge)   AS [Gauge],
TRY_CONVERT (NUMERIC(20, 8), t.Thickness)   AS [Thickness],
TRY_CONVERT (NUMERIC(20, 8), t.Diameter)   AS [Diameter],
NULLIF (t.RawMaterialClassificationType, '') AS [Raw material classification type]
      , ISNULL (NULLIF (UPPER (t.InventoryUOM), ''), 'EA')                                                         AS [Inventory UOM]
      , NULLIF (t.ItemID, '')                                                                                      AS [Item #]
      , ISNULL (NULLIF (t.ItemGroup, ''), 'Other')                                                                 AS [Item group]
      , NULLIF (t.ItemType, '')                                                                                    AS [Item type]
      , t.RecoverableScrap                                                                                         AS [Recoverable scrap]
      , TRY_CONVERT (NUMERIC(20, 8), t.ProductLength)                                                              AS [ProductLength]
      , CASE WHEN t.ProductName = ''
              AND t.ItemID = ''
              THEN ''
              WHEN t.ProductName = ''
              THEN t.ItemID
              WHEN t.ItemID = ''
              THEN t.ProductName
                        ELSE t.ProductName + '  -' + t.ItemID END                                                AS [Product]
      , NULLIF(t.ProductConfig, '')                                                                              AS [Product config]
      , CASE WHEN t.ProductDesc = '' OR t.ProductDesc = '0' THEN NULLIF(t.ProductName, '')ELSE t.ProductDesc END AS [Product desc]
      , NULLIF(LTRIM (t.ProductName), '')                                                                        AS [Product name]
      , t.ProductCategory                                                                                        AS [Product category]
      , TRY_CONVERT(NUMERIC(20, 8), t.ProductWidth)                                                              AS [Product width]
      , CASE WHEN CAST(GETDATE () AS DATE) BETWEEN fd.Date AND td.Date THEN f.StandardPrice ELSE NULL END        AS [Standard price]
    FROM {{ ref("ProductAttribute") }}                t 
  INNER JOIN {{ ref("LegalEntity") }}    le
      ON le.LegalEntityID = t.LegalEntityID
    LEFT JOIN {{ ref("ProductPrice_Fact") }} f
      ON f.ProductKey      = t.ProductKey
    AND f.LegalEntityKey  = le.LegalEntityKey
    LEFT JOIN {{ ref("Date") }}              fd
      ON fd.DateKey        = f.FromDateKey
    LEFT JOIN {{ ref("Date") }}              td
      ON td.DateKey        = f.ToDateKey;
